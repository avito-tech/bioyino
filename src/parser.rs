use std::fmt::Debug;
use std::ops::{Add, AddAssign, Div, Mul, Neg, Sub, SubAssign};
use std::str::from_utf8;
use std::str::FromStr;

use combine::byte::{byte, bytes, newline};
use combine::combinator::{eof, skip_many};
use combine::error::UnexpectedParse;
use combine::parser::byte::digit;
use combine::parser::range::{recognize, take_while1};
use combine::{optional, skip_many1, Parser};

use metric::MetricType;
use std::collections::HashSet;

// to make his zero-copy and get better errors, parser only recognizes parts
// of the metric: (name, value, type, sampling)
pub fn metric_parser<'a, F>(
) -> impl Parser<Output = (&'a [u8], F, MetricType<F>, Option<f32>), Input = &'a [u8]>
where
    F: FromStr
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + Mul<Output = F>
        + Neg<Output = F>
        + PartialOrd
        + Into<f64>
        + From<f64>
        + Debug
        + Default
        + Clone
        + Copy
        + PartialEq
        + Sync,
{
    // This will parse metric name and separator
    let name = take_while1(|c: u8| c != b':' && c != b'\n').skip(byte(b':'));
    let sign = byte(b'+').map(|_| 1i8).or(byte(b'-').map(|_| -1i8));

    // This should parse metric value and separator
    let value = take_while1(|c: u8| c != b'|' && c != b'\n')
        .skip(byte(b'|'))
        .and_then(|value| {
            from_utf8(value)
                .map_err(|_e| UnexpectedParse::Unexpected)
                .map(|v| v.parse::<F>().map_err(|_e| UnexpectedParse::Unexpected))?
        });

    // This parses metric type
    let mtype = bytes(b"ms")
        .map(|_| MetricType::Timer(Vec::<F>::new()))
        .or(byte(b'g').map(|_| MetricType::Gauge(None)))
        .or(byte(b'C').map(|_| MetricType::DiffCounter(F::default())))
        .or(byte(b'c').map(|_| MetricType::Counter))
        .or(byte(b's').map(|_| MetricType::Set(HashSet::new())))
        // we can add more types  here
        //        .or(byte(b'h').map(|_| MetricType::Histrogram))
        ;

    let unsigned_float = skip_many1(digit())
        .and(optional((byte(b'.'), skip_many1(digit()))))
        .and(optional((
            byte(b'e'),
            optional(byte(b'+').or(byte(b'-'))),
            skip_many1(digit()),
        )));

    let sampling = (bytes(b"|@"), recognize(unsigned_float)).and_then(|(_, value)| {
        // TODO replace from_utf8 with handmade parser removing recognize
        from_utf8(value)
            .map_err(|_e| UnexpectedParse::Unexpected)
            .map(|v| v.parse::<f32>().map_err(|_e| UnexpectedParse::Unexpected))?
    });
    (
        name,
        optional(sign),
        value,
        mtype,
        optional(sampling),
        skip_many(newline()).or(eof()),
    )
        .and_then(|(name, sign, mut value, mtype, sampling, _)| {
            let mtype = if let MetricType::Gauge(_) = mtype {
                MetricType::Gauge(sign)
            } else {
                if sign == Some(-1) {
                    // get negative values back
                    value = value.neg()
                }
                mtype
            };

            Ok::<_, UnexpectedParse>((name, value, mtype, sampling))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: Questioned cases:
    //  * negative counters
    //  * diff counters

    #[test]
    fn parse_metric_good_counter() {
        let data = b"gorets:1|c|@1";
        let mut parser = metric_parser::<f64>();
        let (v, rest) = parser.parse(data).unwrap();
        assert_eq!(v.0, b"gorets");
        assert_eq!(v.1, 1f64);
        assert_eq!(v.2, MetricType::Counter);
        assert_eq!(v.3, Some(1f32));
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn parse_metric_good_counter_float() {
        let data = b"gorets:12.65|c|@0.001";
        let mut parser = metric_parser::<f64>();
        let (v, rest) = parser.parse(data).unwrap();
        assert_eq!(v.0, b"gorets");
        assert_eq!(v.1, 12.65f64);
        assert_eq!(v.2, MetricType::Counter);
        assert_eq!(v.3, Some(1e-3f32));
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn parse_metric_with_newline() {
        let data = b"complex.bioyino.test:-1e10|g\n";
        let mut parser = metric_parser::<f64>();
        let (v, rest) = parser.parse(data).unwrap();
        assert_eq!(v.0, b"complex.bioyino.test");
        assert_eq!(v.1, 1e10f64);
        assert_eq!(v.2, MetricType::Gauge(Some(-1i8)));
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn parse_metric_without_newline() {
        let data = b"complex.bioyino.test1:-1e10|gcomplex.bioyino.test10:-1e10|g\n";
        let mut parser = metric_parser::<f64>();
        let (v, rest) = parser.parse(data).unwrap();
        assert_eq!(v.0, b"complex.bioyino.test1");
        assert_eq!(v.1, 1e10f64);
        assert_eq!(v.2, MetricType::Gauge(Some(-1i8)));
        let (v, rest) = parser.parse(rest).unwrap();
        assert_eq!(v.0, b"complex.bioyino.test10");
        assert_eq!(v.1, 1e10f64);
        assert_eq!(v.2, MetricType::Gauge(Some(-1i8)));
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn parse_metric_without_newline_sampling() {
        let data = b"gorets:+1000|g|@0.4e-3gorets:-1000|g|@0.5";
        let mut parser = metric_parser::<f64>();
        let (v, rest) = parser.parse(data).unwrap();
        assert_eq!(v.0, b"gorets");
        assert_eq!(v.1, 1000f64);
        assert_eq!(v.2, MetricType::Gauge(Some(1i8)));
        assert_eq!(v.3, Some(0.0004f32));
        //assert_neq!(rest.len(), 0)
        let (v, rest) = parser.parse(rest).unwrap();
        assert_eq!(v.0, b"gorets");
        assert_eq!(v.1, 1000f64);
        assert_eq!(v.2, MetricType::Gauge(Some(-1i8)));
        assert_eq!(v.3, Some(0.5f32));
        assert_eq!(rest.len(), 0)
    }

    #[test]
    fn parse_metric_short() {
        let data = b"gorets:1|c";
        let mut parser = metric_parser::<f64>();
        let (v, rest) = parser.parse(data).unwrap();
        assert_eq!(v.0, b"gorets");
        assert_eq!(v.1, 1f64);
        assert_eq!(v.2, MetricType::Counter);
        assert_eq!(v.3, None);
        assert_eq!(rest.len(), 0)
    }

    #[test]
    fn parse_metric_many() {
        let data = b"gorets:+1000|g\ngorets:-1000|g|@0.5";
        let mut parser = metric_parser::<f64>();
        let (v, rest) = parser.parse(data).unwrap();
        assert_eq!(v.0, b"gorets");
        assert_eq!(v.1, 1000f64);
        assert_eq!(v.2, MetricType::Gauge(Some(1i8)));
        assert_eq!(v.3, None);
        //assert_neq!(rest.len(), 0)
        let (v, rest) = parser.parse(rest).unwrap();
        assert_eq!(v.0, b"gorets");
        assert_eq!(v.1, 1000f64);
        assert_eq!(v.2, MetricType::Gauge(Some(-1i8)));
        assert_eq!(v.3, Some(0.5f32));
        assert_eq!(rest.len(), 0)
    }
}
