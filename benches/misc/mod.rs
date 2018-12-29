// Copyright 2016 PingCAP, Inc.
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

#![feature(test)]

extern crate arrow;
extern crate byteorder;
extern crate crossbeam;
extern crate futures;
extern crate kvproto;
extern crate log;
extern crate mio;
extern crate num_traits;
extern crate protobuf;
extern crate raft;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate test;
extern crate tipb;

extern crate cop_datatype;
extern crate test_coprocessor;
extern crate test_storage;
extern crate test_util;
extern crate tikv;

extern crate backtrace;
mod channel;
mod coprocessor;
mod raftkv;
mod serialization;
mod storage;
mod writebatch;

#[bench]
fn _bench_check_requirement(_: &mut test::Bencher) {
    tikv::util::config::check_max_open_fds(4096).unwrap();
}

#[test]
fn t(){
    use tikv::util::jemalloc;
    println!("tttt");
    let s = jemalloc::dump_stats();
    let bt = backtrace::Backtrace::new();
    println!("{:?}",bt);

    println!("{}",s);


    println!("===========================================");
    let bt = backtrace::Backtrace::new();

    let s = jemalloc::dump_stats();
    println!("{}",s);
    println!("{:?}",bt);

    println!("===========================================");
    let bt = backtrace::Backtrace::new();
    let bt = backtrace::Backtrace::new();
    let s = jemalloc::dump_stats();
    println!("{}",s);
    println!("s:len,{}",s.len());


    println!("===========================================");
    let bt = backtrace::Backtrace::new();
    let bt = backtrace::Backtrace::new();
    let bt = backtrace::Backtrace::new();

    let s = jemalloc::dump_stats();
    println!("{}",s);
    println!("s:len,{}",s.len());

    println!("eeeeeeeeeeeeeeeeeend");
}

#[test]
fn backtrace_test(){
    use backtrace::Backtrace;
    let mut buf = Vec::with_capacity(10);
    let dumps_result = Backtrace::dumps_emergency(&mut buf);

    if dumps_result.is_err() {
        println!("dumps error: {:?}", dumps_result);
    } else {
        println!("{}", String::from_utf8(buf).unwrap());
    }
}