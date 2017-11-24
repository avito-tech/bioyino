//use errors::*;
use std::ops::{Add, Sub, AddAssign, SubAssign, Div};
use std::fmt::{Display, Debug};
use smallvec::SmallVec;
use failure::Error;
use quantiles::ckms::CKMS;

pub type TimerVec<F> = SmallVec<[F; 32]>;

#[derive(Debug, Clone, PartialEq)]
pub enum MetricType<F>
where
    F: Copy + PartialEq + Debug,
{
    Counter,
    //ExtCounter,
    Timer(CKMS<F>),
    //    Histogram,
    Gauge(Option<i8>),
    //    Set(HashSet<MetricValue>),
}

#[derive(Debug, Clone)]
pub struct Metric<F>
where
    F: Copy + PartialEq + Debug,
{
    pub value: F,
    pub mtype: MetricType<F>,
    sampling: Option<f32>,
}

#[derive(Fail, Debug)]
pub enum MetricError {
    #[fail(display = "float conversion")]
    FloatToRatio,

    #[fail(display = "bad sampling range")]
    Sampling,

    #[fail(display = "aggregating metrics of dirrerent types")]
    Aggregating,
}

impl<F> IntoIterator for Metric<F>
where
    F: Debug
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + Clone
        + Copy
        + PartialOrd
        + PartialEq
        + Into<f64>
        + Display
        + ToString,
{
    type Item = (&'static str, String);
    type IntoIter = MetricIter<F>;
    fn into_iter(self) -> Self::IntoIter {
        MetricIter::new(self)
    }
}

pub struct MetricIter<F>
where
    F: Debug
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + Clone
        + Copy
        + PartialOrd
        + PartialEq
        + Into<f64>
        + Display
        + ToString,
{
    m: Metric<F>,
    count: usize,
}

impl<F> MetricIter<F>
where
    F: Debug
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + Clone
        + Copy
        + PartialOrd
        + PartialEq
        + Into<f64>
        + Display
        + ToString,
{
    fn new(metric: Metric<F>) -> Self {
        MetricIter {
            m: metric,
            count: 0,
        }
    }
}
impl<F> Iterator for MetricIter<F>
where
    F: Debug
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + Clone
        + Copy
        + PartialOrd
        + PartialEq
        + Into<f64>
        + Display
        + ToString,
{
    type Item = (&'static str, String);

    fn next(&mut self) -> Option<Self::Item> {
        let res = match &self.m.mtype {
            &MetricType::Counter if self.count == 0 => Some(("", self.m.value.to_string())),
            &MetricType::Gauge(_) if self.count == 0 => Some(("", self.m.value.to_string())),
            &MetricType::Timer(ref ckms) => {
                // it is safe to unwrap query here because it only fails when len = 0
                // for us zero length is impossible, because metric wouldn't exist in this case
                match self.count {
                    0 => Some(("", self.m.value.to_string())),
                    1 => Some((".count", ckms.count().to_string())),
                    2 => Some((".min", ckms.query(0f64).unwrap().0.to_string())),
                    3 => Some((".max", ckms.query(1f64).unwrap().0.to_string())),
                    4 => Some((".sum", ckms.sum().unwrap().to_string())),
                    5 => Some((".mean", ckms.query(0.5).unwrap().0.to_string())),
                    6 => Some((
                        ".median",
                        (ckms.sum().unwrap().into() / ckms.count() as f64)
                            .to_string(),
                    )),
                    7 => Some((".percentile.75", ckms.query(0.75).unwrap().0.to_string())),
                    8 => Some((".percentile.95", ckms.query(0.95).unwrap().0.to_string())),
                    9 => Some((".percentile.98", ckms.query(0.98).unwrap().0.to_string())),
                    10 => Some((".percentile.99", ckms.query(0.99).unwrap().0.to_string())),
                    11 => Some((
                        ".percentile.999",
                        ckms.query(0.999).unwrap().0.to_string(),
                    )),
                    _ => None,
                }
            }
            _ => None,
        };
        self.count += 1;
        res
    }
}
//impl<F> Metric<F> {
//pub fn flush(&mut self) -> SmallVec<[(&'static str, f64); 32]> {
//use self::MetricType::*;
//let mut res = SmallVec::new();
//match &mut self.mtype {
//&mut Counter => res.push(("", self.value.clone())),
//&mut Gauge(_) => res.push(("", self.value.clone())),
//&mut Timer(ref mut vec) => {
//let len = vec.len();
//if len < 2 {
//res.push(("", self.value.clone()));
//} else {
////vec.sort_unstable();
//res.push((".count", len as f64));
////res.push((".min", *vec.iter().min().unwrap()));
//// TODO res.push((".max", *vec.iter().max().unwrap()));

//let sum = vec.iter().fold(0f64, |acc, x| acc + x);

//res.push((".sum", sum));
//res.push((".mean", sum / len as f64));

//let median = if len % 2 == 1 {
//vec[len / 2 as usize].clone()
//} else {
//(vec[len / 2 as usize - 1].clone() + vec[len / 2 as usize].clone()) / 2f64
//};

//res.push((".median", median));
//res.push((".percentile.75", Percentile::<f64>::count(&vec, 0.75)));
//// res.push((".percentile.95", Percentile(&vec, 0.95)));
////res.push((".percentile.98", percentile(&vec, 0.98)));
////res.push((".percentile.99", percentile(&vec, 0.99)));
//// res.push((".percentile.999", percentile(&vec, 0.999)));
//}
//}
////&mut Set(ref mut vec) => {
////}
//};
//res
//}
//}

impl<F> Metric<F>
where
    F: Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Clone
        + Div<Output = F>
        + PartialOrd
        + PartialEq
        + Into<f64>
        + Copy
        + Debug
        + Sync,
{
    pub fn new(value: F, mtype: MetricType<F>, sampling: Option<f32>) -> Result<Self, Error> {
        let mut metric = Metric {
            value: value,
            mtype: mtype,
            sampling: sampling,
        };

        if let MetricType::Timer(ref mut ckms) = metric.mtype {
            ckms.insert(metric.value.clone());
        };
        // if let MetricType::Set(ref mut set) = metric.mtype {
        //set.insert(metric.value.clone());
        // };
        Ok(metric)
    }


    pub fn aggregate(&mut self, new: Metric<F>) -> Result<(), Error> {
        use self::MetricType::*;
        match (&mut self.mtype, new.mtype) {
            (&mut Counter, Counter) => {
                self.value += new.value;
            }
            (&mut Gauge(_), Gauge(Some(1))) => {
                self.value += new.value;
            }
            (&mut Gauge(_), Gauge(Some(-1))) => {
                self.value -= new.value;
            }
            (&mut Gauge(_), Gauge(None)) => {
                self.value = new.value;
            }
            (&mut Gauge(_), Gauge(Some(_))) => unreachable!(),
            (&mut Timer(ref mut ckms), Timer(_)) => {
                self.value = new.value;
                ckms.insert(new.value)
            }
            //(&mut Set(ref mut set), Set(_)) => {
            //    set.insert(new.value);
            //}
            (m1, m2) => {
                return Err(MetricError::Aggregating.into());
            }
        };
        Ok(())
    }
}

pub struct Percentile<F>(F);

impl Percentile<f64> {
    // Percentile counter. Not safe. Requires at least two elements in vector
    // vector must be sorted
    pub fn count(vec: &TimerVec<f64>, nth: f32) -> f64 {
        let len = vec.len() as f32;
        if vec.len() == 1 {
            return vec[0];
        }

        let k: f32 = nth * (len - 1f32);
        let f = k.floor();
        let c = k.ceil();

        if c == f {
            return vec[k as usize];
        }

        let m0 = c - k;
        let m1 = k - f;
        let d0 = vec[f as usize] * m0 as f64;
        let d1 = vec[c as usize] * m1 as f64;
        let res = d0 + d1;
        res
    }
}
