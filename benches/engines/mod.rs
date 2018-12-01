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

#[macro_use]
extern crate criterion;
extern crate kvproto;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate test_util;
extern crate tikv;

use criterion::{black_box, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use std::fmt;
use test_util::*;
use tikv::storage::engine::{
    BTreeEngine, Engine, Modify, RocksEngine, Snapshot, TestEngineBuilder,
};
use tikv::storage::{Key, Value, CF_DEFAULT};

const DEFAULT_KEY_LENGTH: usize = 64;
const DEFAULT_GET_KEYS_COUNT: usize = 1;
const DEFAULT_PUT_KVS_COUNT: usize = 1;

trait EngineFactory<E: Engine>: Clone + Copy + fmt::Debug + 'static {
    fn build(&self) -> E;
}

#[derive(Clone, Copy)]
struct BTreeEngineFactory {}

impl EngineFactory<BTreeEngine> for BTreeEngineFactory {
    fn build(&self) -> BTreeEngine {
        BTreeEngine::default()
    }
}

impl fmt::Debug for BTreeEngineFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BTreeEngine")
    }
}

#[derive(Clone, Copy)]
struct RocksEngineFactory {}

impl EngineFactory<RocksEngine> for RocksEngineFactory {
    fn build(&self) -> RocksEngine {
        TestEngineBuilder::new().build().unwrap()
    }
}

impl fmt::Debug for RocksEngineFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksEngine")
    }
}

fn fill_engine_with<E: Engine>(engine: &E, expect_engine_keys_count: usize, value_length: usize) {
    if expect_engine_keys_count > 0 {
        let mut modifies: Vec<Modify> = vec![];
        let kvs =
            generate_random_kvs(expect_engine_keys_count, DEFAULT_KEY_LENGTH, value_length);
        for (key, value) in kvs {
            modifies.push(Modify::Put(CF_DEFAULT, Key::from_raw(&key), value))
        }
        let ctx = Context::new();
        let _ = engine.async_write(&ctx, modifies, Box::new(move |(_, _)| {}));
    }
}

#[derive(Serialize, Deserialize)]
struct PutConfig<F> {
    #[serde(skip_serializing)]
    factory: F,

    put_count: usize,
    key_length: usize,
    value_length: usize,
}

impl<F> fmt::Debug for PutConfig<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = serde_json::to_string(self).unwrap();
        write!(f, "{}", s.replace("\"", "+"))
    }
}

fn bench_engine_put<E: Engine, F: EngineFactory<E>>(bencher: &mut Bencher, config: &PutConfig<F>) {
    let engine = config.factory.build();
    let ctx = Context::new();
    bencher.iter_with_setup(
        || {
            let test_kvs: Vec<(Key, Value)> =
                generate_random_kvs(config.put_count, DEFAULT_KEY_LENGTH, config.value_length)
                    .iter()
                    .map(|(key, value)| (Key::from_raw(&key), value.clone()))
                    .collect();
            (test_kvs, &ctx)
        },
        |(test_kvs, ctx)| {
            for (key, value) in test_kvs {
                black_box(engine.put(ctx, key, value).is_ok());
            }
        },
    );
}

fn bench_engine_write<E: Engine, F: EngineFactory<E>>(
    bencher: &mut Bencher,
    config: &PutConfig<F>,
) {
    let engine = config.factory.build();
    let ctx = Context::new();

    bencher.iter_with_setup(
        || {
            let modifies: Vec<Modify> =
                generate_random_kvs(config.put_count, DEFAULT_KEY_LENGTH, config.value_length)
                    .iter()
                    .map(|(key, value)| Modify::Put(CF_DEFAULT, Key::from_raw(&key), value.clone()))
                    .collect();
            (modifies, &ctx)
        },
        |(modifies, ctx)| {
            for modify in modifies {
                black_box(engine.write(ctx, vec![modify]).is_ok());
            }
        },
    );
}

#[derive(Serialize, Deserialize)]
struct SnapshotConfig<F> {
    #[serde(skip_serializing)]
    factory: F,

    engine_keys_count: usize,
    key_length: usize,
    value_length: usize,
}

impl<F> fmt::Debug for GetConfig<F> {
    fn fmt(&self, f: &mut ::fmt::Formatter) -> fmt::Result {
        let s = serde_json::to_string(self).unwrap();
        write!(f, "{}", s.replace("\"", "+"))
    }
}

fn bench_engine_snapshot<E: Engine, F: EngineFactory<E>>(
    bencher: &mut Bencher,
    config: &SnapshotConfig<F>,
) {
    let engine = config.factory.build();
    let ctx = Context::new();
    fill_engine_with(&engine, config.engine_keys_count, config.engine_keys_count);
    bencher.iter(|| black_box(&engine).snapshot(black_box(&ctx)).unwrap());
}

#[derive(Serialize, Deserialize)]
struct GetConfig<F> {
    #[serde(skip_serializing)]
    factory: F,

    get_count: usize,
    key_length: usize,
    value_length: usize,
    engine_keys_count: usize,
}

impl<F> fmt::Debug for GetConfig<F> {
    fn fmt(&self, f: &mut ::fmt::Formatter) -> fmt::Result {
        let s = serde_json::to_string(self).unwrap();
        write!(f, "{}", s.replace("\"", "+"))
    }
}

fn bench_engine_get<E: Engine, F: EngineFactory<E>>(bencher: &mut Bencher, config: &GetConfig<F>) {
    let engine = config.factory.build();
    let ctx = Context::new();
    fill_engine_with(&engine, config.engine_keys_count, config.value_length);
    let test_kvs: Vec<Key> =
        generate_random_kvs(config.get_count, DEFAULT_KEY_LENGTH, config.value_length)
            .iter()
            .map(|(key, _)| Key::from_raw(&key))
            .collect();

    bencher.iter_with_setup(
        || {
            let snap = engine.snapshot(&ctx).unwrap();
            (snap, &test_kvs)
        },
        |(snap, test_kvs)| {
            for key in test_kvs {
                black_box(snap.get(key).unwrap());
            }
        },
    );
}

fn bench_engines<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, factory: F) {
    let value_lengths = vec![64, 65, 1024, 16 * 1024];
    let engine_entries_counts = vec![0, 0];
    let engine_put_kv_counts = vec![DEFAULT_PUT_KVS_COUNT];
    let engine_get_key_counts = vec![DEFAULT_GET_KEYS_COUNT];

    let mut get_configs = vec![];
    let mut put_configs = vec![];
    let mut write_configs = vec![];
    let mut snapshot_configs = vec![];

    for &value_length in &value_lengths {
        for &engine_keys_count in &engine_entries_counts {
            for &get_count in &engine_get_key_counts {
                get_configs.push(GetConfig {
                    factory,
                    get_count,
                    key_length: DEFAULT_KEY_LENGTH,
                    value_length,
                    engine_keys_count,
                });
            }
            snapshot_configs.push(SnapshotConfig {
                factory,
                value_length,
                engine_keys_count,
            });
        }

        for &put_count in &engine_put_kv_counts {
            put_configs.push(PutConfig {
                factory,
                put_count,
                key_length: DEFAULT_KEY_LENGTH,
                value_length,
            });

            write_configs.push(PutConfig {
                factory,
                put_count,
                key_length: DEFAULT_KEY_LENGTH,
                value_length,
            });
        }
    }

    c.bench_function_over_inputs(
        &get_full_method_name(Level::Engine, "get"),
        bench_engine_get,
        get_configs,
    );
    c.bench_function_over_inputs(
        &get_full_method_name(Level::Engine, "put"),
        bench_engine_put,
        put_configs,
    );

    c.bench_function_over_inputs(
        &get_full_method_name(Level::Engine, "write"),
        bench_engine_write,
        write_configs,
    );

    //    c.bench_function_over_inputs(
    //        "bench_engine_snapshot",
    //        bench_engine_snapshot,
    //        snapshot_configs,
    //    );
}

fn bench_RocksDB(c: &mut Criterion) {
    let factory = RocksEngineFactory {};

    let value_lengths = DEFAULT_VALUE_LENGTHS;
    let engine_entries_counts = vec![0, 0];
    let engine_put_kv_counts = vec![DEFAULT_PUT_KVS_COUNT];
    let engine_get_key_counts = vec![DEFAULT_GET_KEYS_COUNT];

    let mut get_configs = vec![];
    let mut put_configs = vec![];
    let mut write_configs = vec![];
    let mut snapshot_configs = vec![];

    for &value_length in &value_lengths {
        for &engine_keys_count in &engine_entries_counts {
            for &get_count in &engine_get_key_counts {
                get_configs.push(GetConfig {
                    factory,
                    get_count,
                    key_length: DEFAULT_KEY_LENGTH,
                    value_length,
                    engine_keys_count,
                });
            }
            snapshot_configs.push(SnapshotConfig {
                factory,
                key_length,
                value_length,
                engine_keys_count,
            });
        }

        for &put_count in &engine_put_kv_counts {
            put_configs.push(PutConfig {
                factory,
                put_count,
                key_length: DEFAULT_KEY_LENGTH,
                value_length,
            });

            write_configs.push(PutConfig {
                factory,
                put_count,
                key_length: DEFAULT_KEY_LENGTH,
                value_length,
            });
        }
    }

    c.bench_function_over_inputs(
        &get_full_method_name(Level::Engine, "get"),
        bench_engine_get,
        get_configs,
    );
    c.bench_function_over_inputs(
        &get_full_method_name(Level::Engine, "put"),
        bench_engine_put,
        put_configs,
    );

    c.bench_function_over_inputs(
        &get_full_method_name(Level::Engine, "write"),
        bench_engine_write,
        write_configs,
    );

    c.bench_function_over_inputs(
        &get_full_method_name(Level::Engine, "snapshot"),
        bench_engine_snapshot,
        snapshot_configs,
    );
}

criterion_group!(benches, bench_RocksDB);

criterion_main!(benches);

//fn main() {
//    let mut criterion = Criterion::default();
//    bench_engines(&mut criterion, RocksEngineFactory {});
//    //    bench_engines(&mut criterion, BTreeEngineFactory {});
//    criterion.final_summary();
//}
