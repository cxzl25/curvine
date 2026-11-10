// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![recursion_limit = "256"]

//! A cache hit must not depend on the client being able to build the mount's UFS client.
//!
//! This test binary is built with the `opendal-s3` backend only, so the client cannot
//! create a UFS client for an `hdfs://` mount — the same situation as a Java SDK without
//! an HDFS backend, or a JNI HDFS client that cannot start inside a host JVM. Cached data
//! must still be served; the UFS client is only needed if a worker fails mid-read.

use curvine_client::unified::{UfsFileSystem, UnifiedFileSystem, UnifiedReader};
use curvine_core_error::CommonResult;
use curvine_fs_api::{FileSystem, Path, Reader};
use curvine_model::{AccessMode, MountOptionsBuilder, SetAttrOpts, WriteType};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_tests::Testing;
use std::collections::HashMap;
use std::sync::Arc;

const DATA: &str = "cached data without a ufs client";

async fn assert_cache_hit_readable(
    fs: &UnifiedFileSystem,
    write_type: WriteType,
    name: &str,
) -> CommonResult<()> {
    let ufs_path = Path::from_str(format!("hdfs://no-hdfs-backend:8020/{name}"))?;
    let cv_path = Path::from_str(format!("/{name}"))?;
    assert!(
        UfsFileSystem::new(&ufs_path, HashMap::new(), None).is_err(),
        "precondition: this build must not be able to create an hdfs UFS client"
    );

    let opts = MountOptionsBuilder::new()
        .write_type(write_type)
        .access_mode(AccessMode::ReadWrite)
        .auto_cache(false)
        .build();
    fs.mount(&ufs_path, &cv_path, opts).await?;

    // Put the file into Curvine and record a UFS snapshot (what a load job does), so
    // the CacheMode validity check sees a valid cached copy.
    let file = Path::from_str(format!("/{name}/a.txt"))?;
    fs.cv().write_string(&file, DATA).await?;
    let attr = SetAttrOpts {
        ufs_mtime: Some(LocalTime::mills() as i64),
        ..Default::default()
    };
    fs.cv().set_attr(&file, attr).await?;

    let mut reader = fs.open(&file).await?;
    assert!(
        matches!(reader, UnifiedReader::Fallback(_)),
        "{write_type:?}: expected a cache hit served by Curvine"
    );
    assert_eq!(reader.read_as_string().await?, DATA, "{write_type:?}");
    reader.complete().await?;
    Ok(())
}

#[test]
fn cache_hit_does_not_require_ufs_client() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("lazy-ufs-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        assert_cache_hit_readable(&fs, WriteType::FsMode, "lazy-ufs-fs-mode").await?;
        assert_cache_hit_readable(&fs, WriteType::CacheMode, "lazy-ufs-cache-mode").await
    })
}
