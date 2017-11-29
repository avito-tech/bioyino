use metric::{Metric, MetricType};
use std::ops::{Add, Sub, AddAssign, SubAssign, Div};
use std::fmt::Debug;
use std::str::from_utf8;
use std::str::FromStr;
use combine::{Parser, optional};
use combine::byte::{byte, bytes, newline};
use combine::range::{take_while1, take_while};
use combine::combinator::{eof, skip_many};
use quantiles::ckms::CKMS;
use EPSILON;

use failure::{Compat, ResultExt};

#[derive(Fail, Debug)]
enum ParseError {
    #[fail(display = "parsing UTF-8 data")]
    Utf8(
        #[cause]
        ::std::str::Utf8Error
    ),

    #[fail(display = "parsing float")]
    Float(String),
}

pub type MetricParser<'a, F> = Box<Parser<Output = (String, Metric<F>), Input = &'a [u8]>>;
pub fn metric_parser<'a, F>() -> MetricParser<'a, F>
where
    F: FromStr
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + PartialOrd
        + Into<f64>
        + Debug
        + Default
        + Clone
        + Copy
        + PartialEq
        + Sync,
{
    // This will parse metic name and separator
    let name = take_while1(|c: u8| c != b':' && c != b'\n')
        .skip(byte(b':'))
        .and_then(|name| from_utf8(name).map(|name| name.to_string()));

    let sign = byte(b'+').map(|_| 1i8).or(byte(b'-').map(|_| -1i8));

    // This should parse metric value and separator
    let value = take_while1(|c: u8| c != b'|' && c != b'\n')
        .skip(byte(b'|'))
        .and_then(|value| {
            from_utf8(value)
                .map_err(|e| ParseError::Utf8(e))
                .compat()?
                .parse::<F>()
                .map_err(|_| {
                    ParseError::Float(
                        format!("parsing {:?} as float metric value", value).to_string(),
                    )
                })
                .compat()
        });

    // This parses metric type
    let mtype = bytes(b"ms")
        .map(|_| MetricType::Timer(CKMS::<F>::new(EPSILON)))
        .or(byte(b'g').map(|_| MetricType::Gauge(None)))
        .or(byte(b'C').map(|_| MetricType::DiffCounter(F::default())))
        .or(byte(b'c').map(|_| MetricType::Counter))
        //        .or(byte(b's').map(|_| MetricType::Set(HashSet::new())))
        // we can add more types  here
        //        .or(byte(b'h').map(|_| MetricType::Histrogram))
        ;

    let sampling = (bytes(b"|@"), take_while(|c: u8| c != b'\n'))
        .and_then::<_, f32, Compat<ParseError>>(|(_, value)| {
            from_utf8(value)
                .map_err(|e| ParseError::Utf8(e))
                .compat()?
                .parse::<f32>()
                .map_err(|_| {
                    ParseError::Float(
                        format!("parsing {:?} as float sampling value", value).to_string(),
                    )
                })
                .compat()
        });

    let metric = name.and(
        (
            optional(sign),
            value,
            mtype,
            optional(sampling),
            skip_many(newline()).or(eof()),
        ).and_then(|(sign, value, mtype, sampling, _)| {
                let real_mtype = if let MetricType::Gauge(_) = mtype {
                    MetricType::Gauge(sign)
                } else {
                    mtype
                };

                Metric::<F>::new(value, real_mtype, sampling).compat()
            }),
    );

    Box::new(metric)
}

#[cfg(test)]
mod tests {
    // WARNING: these tests most probably don't work as of now
    // FIXME: tests
    use super::*;
    use num::rational::Ratio;

    // TODO; parse bad and tricky metrics
    // Questioned cases:
    //  * non-integer counters
    //  * negative counters

    #[test]
    fn parse_good_counter() {
        let data = b"gorets:1|c|@1";
        let v = parse_metrics(data).unwrap();
        assert_eq!(v[0].0, "gorets".to_string());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].1.mtype, MetricType::Counter);
        assert_eq!(v[0].1.value, Ratio::new(1.into(), 1.into()));
    }

    #[test]
    fn parse_multi_metric_one() {
        let data = b"complex.bioyino.test:1|g\n";
        let v = parse_metrics(data).unwrap();
        assert_eq!(v[0].0, "complex.bioyino.test".to_string());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].1.mtype, MetricType::Gauge(None));
        assert_eq!(v[0].1.value, Ratio::new(1.into(), 1.into()));
    }

    #[test]
    fn parse_multi_metric_many() {
        let data = b"complex.bioyino.test.1:1|g\ncomplex.bioyino.test.2:2|g";
        let v = parse_metrics(data).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].0, "complex.bioyino.test.1".to_string());
        assert_eq!(v[0].1.mtype, MetricType::Gauge(None));
        assert_eq!(v[0].1.value, Ratio::new(1.into(), 1.into()));
        assert_eq!(v[1].0, "complex.bioyino.test.2".to_string());
        assert_eq!(v[1].1.mtype, MetricType::Gauge(None));
        assert_eq!(v[1].1.value, Ratio::new(2.into(), 1.into()));
    }

    #[test]
    fn parse_short_metric() {
        let data = b"gorets:1|c";
        let d = parse_metrics(data);
        let v = d.unwrap();
        assert_eq!(v[0].1.mtype, MetricType::Counter);
        assert_eq!(v[0].1.value, Ratio::new(1.into(), 1.into()));
    }

    #[test]
    fn parse_gauge() {
        let data = b"gorets:-75e-2|g|@1";
        let v = parse_metrics(data).unwrap();
        assert_eq!(v[0].1.mtype, MetricType::Gauge(Some(-1)), "bad type");
        // 0.75 should parse to 3/4
        assert_eq!(v[0].1.value, Ratio::new(3.into(), 4.into()), "bad value");
    }

    #[test]
    fn parse_complex_gauges() {
        let data = b"gorets:+1000|g\ngorets:-1000|g|@0.5";
        let v = parse_metrics(data).unwrap();
        assert_eq!(v[0].1.mtype, MetricType::Gauge(Some(1)));
        assert_eq!(v[0].1.value, Ratio::new(1000.into(), 1.into()));
        assert_eq!(v[1].1.mtype, MetricType::Gauge(Some(-1)), "");
        assert_eq!(v[1].1.value, Ratio::new(1000.into(), 1.into()));
    }
}
