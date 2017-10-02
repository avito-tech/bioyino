use errors::*;
use metric::{Metric, MetricType};

use std::str::{from_utf8};
//use std::collections::HashSet;

use smallvec::SmallVec;
use combine::{Parser, optional, sep_by1};
use combine::byte::{byte, bytes, newline};
use combine::range::take_while;

// We can store at most 16 metrics on stack, this is cool
pub fn parse_metrics(input: &[u8]) -> Result<SmallVec<[(String, Metric); 16]>> {
    // This will parse metic name and separator
    let name = take_while(|c: u8| c != b':')
        .skip(byte(b':'))
        .and_then(|name| from_utf8(name));

    let sign = byte(b'+').map(|_| 1i8)
        .or(byte(b'-').map(|_| -1i8));

    // This should parse metric value and separator
    let value = take_while(|c: u8| c != b'|')
        .skip(byte(b'|'))
        .and_then(|value| from_utf8(value).expect("bad value").parse::<f64>());

    // This parses metric type
    let mtype = bytes(b"ms")
        .map(|_| MetricType::Timer(SmallVec::new()))
        .or(byte(b'g').map(|_| MetricType::Gauge(None)))
        //.or(byte(b'C').map(|_| MetricType::ExtCounter))
        .or(byte(b'c').map(|_| MetricType::Counter))
        //        .or(byte(b's').map(|_| MetricType::Set(HashSet::new())))
        // we can add more types  here
        //        .or(byte(b'h').map(|_| MetricType::Histrogram))
        ;

    let sampling = (byte(b'@'), take_while(|c: u8| c != b'\n')).and_then(|(_, value)| {
        from_utf8(value)
            .expect("sampling is not float")
            .parse::<f64>()
    });

    let metric = (name, optional(sign), value, mtype, optional(sampling))//, try(eof()).or(byte(b'\n').map(|_| ())))
        .map(|(name, sign, value, mtype, sampling)| {
            //
            let real_mtype  = if let MetricType::Gauge(_) = mtype {
                MetricType::Gauge(sign)
            } else { mtype };

            (name.to_string(), Metric::new(value, real_mtype, sampling).expect("creating metric"))
        });

    let mut metrics = sep_by1(metric, newline());
    let (v, _) = metrics.parse(input).map_err(|_| ErrorKind::Parsing)?;
    Ok(v)
}

#[cfg(test)]
mod tests {

    use super::*;
    use num::rational::Ratio;

    // TODO; parse bad and tricky metrics
    // Questioned cases:
    //  * non-integer counters
    //  * negative counters

    #[test]
    fn parse_good_metric() {
        let data = b"gorets:1|c|@1";
        let v = parse_metrics(data).unwrap();
        assert_eq!(v[0].0, "gorets".to_string());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].1.mtype, MetricType::Counter);
        assert_eq!(v[0].1.value, Ratio::new(1.into(), 1.into()));
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
