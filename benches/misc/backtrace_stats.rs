// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;
use backtrace;
struct UnReservedVec {
    inner: Vec<u8>,
    write_cont: usize,
}

impl UnReservedVec {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
            write_cont: 0,
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }
}

impl Drop for UnReservedVec {
    fn drop(&mut self) {
        println!("buf len:{}", self.inner.len());
        println!("{}", String::from_utf8_lossy(&self.inner));
    }
}

impl ::std::io::Write for UnReservedVec {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> ::std::io::Result<usize> {
        if self.inner.capacity() - self.inner.len() >= buf.len() {
            self.write_cont += 1;
            self.inner.extend_from_slice(buf);
            Ok(buf.len())
        } else {
            Ok(0)
        }
    }

    #[inline]
    fn flush(&mut self) -> ::std::io::Result<()> {
        Ok(())
    }
}
/// This test shows that `write!()` doesn't allocate.
#[test]
fn write_state() {
    use tikv::util::jemalloc;

    let mut v = Vec::with_capacity(3024000);
    let before = jemalloc::dump_stats();

    for i in 0..50 {
        write!(v, "{}.{}", before, i);
    }

    let after = jemalloc::dump_stats();
    println!("before_write: \n{}", before);
    println!("after_write: \n{}", after);
    println!("v len:{}", v.len());
}

/// On Linux platformThe, `nrequests` in jemalloc stats of `backtrace_dumps_test` increases 30
/// than `backtrace_no_dumps_test`.
/// On MacOs, the `nrequests`s are same.
#[test]
fn backtrace_dumps_test() {
    use backtrace::Backtrace;
    use tikv::util::jemalloc;

    let bt = backtrace::Backtrace::new();
    let mut buf = UnReservedVec::with_capacity(102400);

    let dumps_result = Backtrace::dumps_emergency(&mut buf);

    let jemalloc_stats = jemalloc::dump_stats();
    println!("jemalloc_stats: \n{}", jemalloc_stats);
}

#[test]
fn backtrace_no_dumps_test() {
    use backtrace::Backtrace;
    use tikv::util::jemalloc;

    let bt = backtrace::Backtrace::new();
    let mut buf = UnReservedVec::with_capacity(102400);

    // write buf
    for _ in 0..20 {
        for _ in 0..170 {
            write!(buf, "{}", 0);
        }
    }
    //    let dumps_result = Backtrace::dumps_emergency(&mut buf);
    let jemalloc_stats = jemalloc::dump_stats();
    println!("jemalloc_stats: \n{}", jemalloc_stats);
}
