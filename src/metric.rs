//use errors::*;
use std::ops::{Add, AddAssign, Div, Mul, Sub, SubAssign};
use std::fmt::{Debug, Display};
use failure::Error;
//use quantiles::ckms::CKMS;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricType<F>
where
    F: Copy + PartialEq + Debug,
{
    Counter,
    DiffCounter(F),
    Timer(Vec<F>),
    //    Histogram,
    Gauge(Option<i8>),
    //    Set(HashSet<MetricValue>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric<F>
where
    F: Copy + PartialEq + Debug,
{
    pub value: F,
    pub mtype: MetricType<F>,
    pub update_counter: u64,
    sampling: Option<f32>,
}

#[derive(Fail, Debug)]
pub enum MetricError {
    #[fail(display = "float conversion")] FloatToRatio,

    #[fail(display = "bad sampling range")] Sampling,

    #[fail(display = "aggregating metrics of different types")] Aggregating,
}

impl<F> IntoIterator for Metric<F>
where
    F: Debug
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + Mul<Output = F>
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
        + Mul<Output = F>
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
    // cached sum to avoid recounting
    timer_sum: Option<F>,
}

impl<F> MetricIter<F>
where
    F: Debug
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + Mul<Output = F>
        + Clone
        + Copy
        + PartialOrd
        + PartialEq
        + Into<f64>
        + Display
        + ToString,
{
    fn new(mut metric: Metric<F>) -> Self {
        let sum = if let MetricType::Timer(ref mut agg) = metric.mtype {
            agg.sort_unstable_by(|ref v1, ref v2| v1.partial_cmp(v2).unwrap());
            let first = agg.first().unwrap();
            Some(agg.iter().skip(1).fold(*first, |acc, &v| acc + v))
        } else {
            None
        };
        MetricIter {
            m: metric,
            count: 0,
            timer_sum: sum,
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
        + Mul<Output = F>
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
            &MetricType::DiffCounter(_) if self.count == 0 => Some(("", self.m.value.to_string())),
            &MetricType::Gauge(_) if self.count == 0 => Some(("", self.m.value.to_string())),
            &MetricType::Timer(ref agg) => {
                // it is safe to unwrap query here because it only fails when len = 0
                // for us zero length is impossible, because metric wouldn't exist in this case
                match self.count {
                    0 => Some(("", self.m.value.to_string())),
                    //1 => Some((".count", ckms.count().to_string())),
                    1 => Some((".count", agg.len().to_string())),
                    //2 => Some((".min", ckms.query(0f64).unwrap().1.to_string())),
                    2 => Some((".min", agg[0].to_string())),
                    //3 => Some((".max", ckms.query(1f64).unwrap().1.to_string())),
                    3 => Some((".max", agg[agg.len() - 1].to_string())),
                    //4 => Some((".sum", ckms.sum().unwrap().to_string())),
                    4 => Some((".sum", self.timer_sum.unwrap().to_string())),
                    //5 => Some((".median", ckms.query(0.5f64).unwrap().1.to_string())),
                    5 => Some((".median", percentile(agg, 0.5).to_string())),
                    6 => Some((
                        ".mean",
                        (self.timer_sum.unwrap().into() / agg.len() as f64).to_string(),
                    )),
                    //7 => Some((
                    //".percentile.75",
                    //ckms.query(0.75f64).unwrap().1.to_string(),
                    //        )),
                    7 => Some((".percentile.75", percentile(agg, 0.75).to_string())),
                    8 => Some((".percentile.95", percentile(agg, 0.95).to_string())),
                    9 => Some((".percentile.98", percentile(agg, 0.98).to_string())),
                    10 => Some((".percentile.99", percentile(agg, 0.99).to_string())),
                    11 => Some((".percentile.999", percentile(agg, 0.999).to_string())),
                    _ => None,
                }
            }
            _ => None,
        };
        self.count += 1;
        res
    }
}

impl<F> Metric<F>
where
    F: Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Clone
        + Div<Output = F>
        + Mul<Output = F>
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
            update_counter: 1
        };

        if let MetricType::Timer(ref mut agg) = metric.mtype {
            // ckms.insert(metric.value);
            agg.push(metric.value)
        };
        // if let MetricType::Set(ref mut set) = metric.mtype {
        //set.insert(metric.value.clone());
        // };
        Ok(metric)
    }


    pub fn aggregate(&mut self, new: Metric<F>) -> Result<(), Error> {
        use self::MetricType::*;
        self.update_counter += new.update_counter;
        match (&mut self.mtype, new.mtype) {
            (&mut Counter, Counter) => {
                self.value += new.value;
            }
            (&mut DiffCounter(ref mut previous), DiffCounter(_)) => {
                // FIXME: this is most probably incorrect when joining with another
                // non-fresh metric count2 != 1
                let prev = *previous;
                let diff = if new.value > prev {
                    new.value - prev
                } else {
                    new.value
                };
                *previous = new.value;
                self.value += diff;
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
            (&mut Gauge(_), Gauge(Some(_))) => {
                return Err(MetricError::Aggregating.into());
            }
            (&mut Timer(ref mut agg), Timer(ref mut agg2)) => {
                self.value = new.value;
                agg.append(agg2);
            }
            //            (&mut Timer(ref mut ckms), Timer(ref ckms2)) => {
            //self.value = new.value;
            //ckms.add_assign(ckms2.clone());
            //}
            //(&mut Set(ref mut set), Set(_)) => {
            //    set.insert(new.value);
            //}
            (_m1, _m2) => {
                return Err(MetricError::Aggregating.into());
            }
        };
        Ok(())
    }
}

// Percentile counter. Not safe. Requires at least two elements in vector
// vector must be sorted
pub fn percentile<F>(vec: &Vec<F>, nth: f64) -> f64
where
    F: Into<f64> + Clone,
{
    let last = (vec.len() - 1) as f64;
    if last == 0f64 {
        return vec[0].clone().into();
    }

    let k: f64 = nth * last;
    let f = k.floor();
    let c = k.ceil();

    if c == f {
        // exact nth percentile have been found
        return vec[k as usize].clone().into();
    }

    let m0 = c - k;
    let m1 = k - f;
    let d0 = vec[f as usize].clone().into() * m0;
    let d1 = vec[c as usize].clone().into() * m1;
    let res = d0 + d1;
    res
}

#[cfg(test)]
mod tests {
    use super::*;
}
