/*
                               cache.alter(name, |old| {
                            //
                            match old {
                            None => Some(metric),
                            Some(mut old) => {
                            let res = old.aggregate(metric);
                            if res.is_err() {
                            AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                            }
                            Some(old)
                            }
                            }
                            });
                            */

/*
   ToString:
// let formatted = match (value.numer().to_f64(), value.denom().to_f64()) {
//(Some(numer), Some(denom)) => (numer / denom).to_string(),
//_ => return Err(ErrorKind::Format.into()),
//};

use metric::{Metric, MetricType, TimerVec, Percentile, MetricError};

use failure::Error;
use smallvec::SmallVec;
use num::{BigInt, ToPrimitive};
use num::rational::Ratio;
use num::traits::{Float, Zero};

impl Metric<Ratio<BigInt>> {
pub fn new<F: Float>(
value: F,
mtype: MetricType<Ratio<BigInt>>,
sampling: Option<f64>,
) -> Result<Self, Error> {
let mut value = Ratio::from_float(value).ok_or(MetricError::FloatToRatio)?;
let sampling = match sampling {
Some(f) => {
let s = Ratio::from_float(f).ok_or(MetricError::FloatToRatio)?;
// Sampling value must be (0;1]
if s.numer() > s.denom() {
return Err(MetricError::Sampling.into());
} else if s.is_zero() {
return Err(MetricError::Sampling.into());
}
value = value * s.clone();
Some(s)
}
None => None,
};

let mut metric = Metric {
value: value,
mtype: mtype,
sampling: sampling,
};

if let MetricType::Timer(ref mut vec) = metric.mtype {
vec.push(metric.value.clone());
};
// if let MetricType::Set(ref mut set) = metric.mtype {
//set.insert(metric.value.clone());
// };
Ok(metric)
}

pub fn flush(&mut self) -> SmallVec<[(&'static str, Ratio<BigInt>); 32]> {
use self::MetricType::*;
let mut res = SmallVec::new();
match &mut self.mtype {
&mut Counter => res.push(("", self.value.clone())),
&mut Gauge(_) => res.push(("", self.value.clone())),
&mut Timer(ref mut vec) => {
let len = vec.len();
if len < 2 {
res.push(("", self.value.clone()));
} else {
vec.sort_unstable();
res.push((".count", Ratio::new(len.into(), 1.into())));
res.push((".min", vec.iter().min().unwrap().clone()));
res.push((".max", vec.iter().max().unwrap().clone()));

let sum = vec.iter().fold(Ratio::zero(), |acc, x| acc + x);

res.push((".sum", sum.clone()));
res.push((".mean", sum / Ratio::new(len.into(), 1.into())));

let median = if len % 2 == 1 {
    vec[len / 2 as usize].clone()
} else {
    (vec[len / 2 as usize - 1].clone() + vec[len / 2 as usize].clone()) /
        Ratio::new(2.into(), 1.into())
};

res.push((".median", median));
res.push((
        ".percentile.75",
        Percentile::<Ratio<BigInt>>::count(&vec, 0.75),
        ));
//res.push((".percentile.95", percentile(&vec, 0.95)));
//res.push((".percentile.98", percentile(&vec, 0.98)));
//res.push((".percentile.99", percentile(&vec, 0.99)));
//res.push((".percentile.999", percentile(&vec, 0.999)));
}
}
//&mut Set(ref mut vec) => {
//}
};
res
}
}

impl Percentile<Ratio<BigInt>> {
    // Percentile counter. Not safe. Requires at least two elements in vector
    // vector must be sorted
    pub fn count(vec: &TimerVec<Ratio<BigInt>>, nth: f32) -> Ratio<BigInt> {
        let len = vec.len() as f32;
        if vec.len() == 1 {
            return vec[0].clone();
        }

        let k: f32 = nth * (len - 1f32);
        let f = k.floor();
        let c = k.ceil();

        if c == f {
            return vec[k as usize].clone();
        }

        let m0 = Ratio::from_float(c - k).unwrap();
        let m1 = Ratio::from_float(k - f).unwrap();
        let d0 = vec[f as usize].clone() * m0;
        let d1 = vec[c as usize].clone() * m1;
        let res = d0 + d1;
        res
    }
}

pub fn ratio_f64(value: &Ratio<BigInt>) -> f64 {
    value.numer().to_f64().unwrap() / value.denom().to_f64().unwrap()
}

#[cfg(test)]
mod tests {

    use super::*;
    use parser::parse_metrics;

    #[test]
    fn aggregate_counter() {
        let mut old = parse_metrics(b"metric1:34|c").unwrap().pop().unwrap().1;
        let new = parse_metrics(b"metric1:34|c").unwrap().pop().unwrap().1;
        old.aggregate(new).unwrap();
        assert_eq!(old.value, Ratio::new(68.into(), 1.into()));

        let mut old = parse_metrics(b"metric1:3.4|c").unwrap().pop().unwrap().1;
        let new = parse_metrics(b"metric1:3.4|c").unwrap().pop().unwrap().1;
        old.aggregate(new).unwrap();

        // We can do only approximate comparison
        let left: MetricValue = (680000.into(), 99999.into()).into();
        let right: MetricValue = (680000.into(), 100001.into()).into();
        assert!(old.value < left);
        assert!(old.value > right);
        assert!(ratio_f64(&old.value) == 6.8);

        let mut old = parse_metrics(b"metric1:34|c").unwrap().pop().unwrap().1;
        let new = parse_metrics(b"metric1:34|g").unwrap().pop().unwrap().1;
        assert!(old.aggregate(new).is_err());
    }

    #[test]
    fn aggregate_gauge() {
        let mut old = parse_metrics(b"metric1:3.4|g").unwrap().pop().unwrap().1;
        let new = parse_metrics(b"metric1:4.6|g").unwrap().pop().unwrap().1;
        old.aggregate(new).unwrap();

        assert!(ratio_f64(&old.value) == 4.6);

        let mut old = parse_metrics(b"metric1:3.4|g").unwrap().pop().unwrap().1;
        let new = parse_metrics(b"metric1:+4.7|g").unwrap().pop().unwrap().1;
        old.aggregate(new).unwrap();

        assert!(ratio_f64(&old.value) == 8.1);

        let mut old = parse_metrics(b"metric1:3.4|g").unwrap().pop().unwrap().1;
        let new = parse_metrics(b"metric1:-4.7|g").unwrap().pop().unwrap().1;
        use num::traits::Signed;
        assert!(old.value.is_positive());
        assert!(new.value.is_positive());
        old.aggregate(new).unwrap();

        // We can do only approximate comparison
        let left: MetricValue = (130000.into(), 99999.into()).into();
        let right: MetricValue = (130000.into(), 100001.into()).into();
        use std::ops::Neg;
        assert!(old.value > left.neg());
        assert!(old.value < right.neg());
    }

    // TODO: not sure we should test aggregation for timers and sets, since they are obvious
    #[test]
    fn find_percentile() {
        let mut old = parse_metrics(b"metric1:20|ms").unwrap().pop().unwrap().1;
        let news = vec![
            b"metric1:1|ms",
            b"metric1:9|ms",
            b"metric1:5|ms",
            b"metric1:8|ms",
            b"metric1:3|ms",
            b"metric1:7|ms",
        ];
        news.into_iter()
            .map(|s| {
                let new = parse_metrics(s).unwrap().pop().unwrap().1;
                &mut old.aggregate(new);
            })
        .last();
        if let MetricType::Timer(mut vec) = old.mtype {
            vec.sort();

            // Used python to count these. not actually sure about it
            let perc = percentile(&vec, 0.95);

            let left: MetricValue = (1670000.into(), 99999.into()).into();
            let right: MetricValue = (1670000.into(), 100001.into()).into();
            assert!(perc < left);
            assert!(perc > right);

            let perc = percentile(&vec, 0.05);

            let left: MetricValue = (160000.into(), 99999.into()).into();
            let right: MetricValue = (160000.into(), 100001.into()).into();
            assert!(perc < left);
            assert!(perc > right);

            let perc = percentile(&vec, 0.35);

            let left: MetricValue = (520000.into(), 99999.into()).into();
            let right: MetricValue = (520000.into(), 100001.into()).into();
            assert!(perc < left);
            assert!(perc > right);
        } else {
            panic!("wrong metric type");
        }
    }

}
*/
