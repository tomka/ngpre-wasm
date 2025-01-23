use async_trait::async_trait;

use futures::future::TryFutureExt;
use std::fmt::Write;
use std::str::FromStr;
use std::{cmp, io};
use std::ops::Range;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use futures::future::join_all;

use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{console, Performance, Request, RequestInit, RequestMode, Response};

use super::*;
use ngpre::{DataLoader, DataLoaderResult, decode};

const ATTRIBUTES_FILE: &str = "info";

enum GlobalProxy {
    Window(web_sys::Window),
    WorkerGlobalScope(web_sys::WorkerGlobalScope),
}

impl GlobalProxy {
    fn fetch_with_request(&self, request: &Request) -> Promise {
        match self {
            GlobalProxy::Window(window) => window.fetch_with_request(request),
            GlobalProxy::WorkerGlobalScope(scope) => scope.fetch_with_request(request),
        }
    }

    fn performance(&self) -> Performance {
        match self {
            GlobalProxy::Window(window) => window.performance().unwrap(),
            GlobalProxy::WorkerGlobalScope(scope) => scope.performance().unwrap(),
        }
    }
}

fn self_() -> Result<GlobalProxy, JsValue> {
    let global = js_sys::global();
    if js_sys::eval("typeof WorkerGlobalScope !== 'undefined'")?.as_bool().unwrap() {
        return Ok(global.dyn_into::<web_sys::WorkerGlobalScope>().map(GlobalProxy::WorkerGlobalScope)?)
    }

    Ok(global.dyn_into::<web_sys::Window>().map(GlobalProxy::Window)?)
}

fn perf_to_system(amt: f64) -> SystemTime {
    let secs = (amt as u64) / 1_000;
    let nanos = (((amt as u64) % 1_000) as u32) * 1_000_000;
    UNIX_EPOCH + Duration::new(secs, nanos)
}

#[wasm_bindgen]
pub struct NgPreHTTPFetch {
    base_path: String,
}

impl NgPreHTTPFetch {
    fn fetch(&self, path_name: &str, use_cors: bool) -> JsFuture {
        utils::set_panic_hook();
        let request_options = RequestInit::new();
        request_options.set_method("GET");
        let request_mode = if use_cors { RequestMode::Cors} else { RequestMode::NoCors };
        request_options.set_mode(request_mode);

        let req = Request::new_with_str_and_init(
            &format!("{}/{}", &self.base_path, path_name),
            &request_options).expect("to create request object");

        JsFuture::from(self_().unwrap().fetch_with_request(&req))
    }

    async fn fetch_json(&self, path_name: &str, use_cors: bool) -> JsValue {
        utils::set_panic_hook();
        let resp_value = self.fetch(path_name, use_cors).await.expect("to wait on JavaScript promise");
        assert!(resp_value.is_instance_of::<Response>());
        let resp: Response = resp_value.dyn_into().expect("failed to convert the promise into Response type");

        JsFuture::from(resp.json().unwrap()).await.expect("failed parse JSON")
    }

    async fn get_attributes(&self, path_name: &str) -> serde_json::Value {
        utils::set_panic_hook();
        let path = self.get_dataset_attributes_path(path_name);
        let js_val = self.fetch_json(&path, true).await;
        serde_wasm_bindgen::from_value(js_val).expect("failed to convert javascript type into rust type")
    }

    fn relative_block_path(&self, path_name: &str, grid_position: &[i64], block_size: &[u32], voxel_offset: &[i32], dimensions: &[u64]) -> String {
        let mut block_path = path_name.to_owned();
        let mut n = 0;
        write!(block_path, "/").unwrap();
        for coord in grid_position {
            if n > 0 {
                write!(block_path, "_").unwrap();
            }
            // This assumes 0 <= coord <= grid_size[n].
            // write!(block_path, "x{}x{}x{}x", voxel_offset[n], coord, block_size[n]).unwrap();
            let begin_offset = voxel_offset[n] as i64 + coord * block_size[n] as i64;
            let end_offset = voxel_offset[n] as i64 + cmp::min(
                     (coord + 1) * block_size[n] as i64, dimensions[n] as i64);

            write!(block_path, "{}-{}", begin_offset, end_offset).unwrap();
            n = n + 1;
        }

        block_path
    }

    fn get_dataset_attributes_path(&self, path_name: &str) -> String {
        if path_name.is_empty() {
            return ATTRIBUTES_FILE.to_string()
        }

        // There is only one top-level attribute file
        ATTRIBUTES_FILE.to_string()
    }

    async fn optimize_requests<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_coords: Vec<UnboundedGridCoord>,
    ) -> Vec<Vec<UnboundedGridCoord>>
    where
        T: ReflectedType,
    {
        // FIXME: Input should be bounded already
        let bounded_grid_coords = grid_coords.iter().map(|coords| GridCoord::from_vec(coords.iter().map(|x| *x as u64).collect())).collect();

        // TODO: Could be nicer
        let zoom_level = data_attrs.get_scales().iter().position(|s| s.key == path_name).unwrap();
        let chunk_size = data_attrs.get_block_size(zoom_level);

        // For non-sharded data, there currently is no real optimization to be done.
        if !data_attrs.is_sharded(zoom_level) {
            return vec![grid_coords];
        }

        let meta = ngpre::PrecomputedMetadata {
            cloudpath: self.base_path.to_string(),
        };

        let bounds = data_attrs.bounds(zoom_level);
        let grid_size: Vec<u64> = bounds[0].iter().zip(bounds[1].iter()).enumerate()
            .map(|(i, (bmin, bmax))| ((bmax - bmin) as u64).div_ceil(chunk_size[i].into()))
            .collect();
        let gpts = bounded_grid_coords;
        let morton_codes = ngpre::compressed_morton_code(&gpts, &grid_size).unwrap();

        // Get raw data and send back
        let progress = false;
        let decompress = false;
        let parallel = 0;

        let loader = HTTPDataLoader {};
        let cache = ngpre::CacheService {
            enabled: false,
            data_loader: &loader,
            meta: &meta,
        };
        let spec = data_attrs.get_sharding_spec(zoom_level).unwrap();
        let mut reader = ngpre::ShardReader::new(&meta, &cache, &spec, &loader, None, None);

        let data_key = data_attrs.get_key(zoom_level);
        let optimized_bundles = reader.get_bundled_grid_coords(
            &morton_codes,
            Some(data_key),
            Some(progress),
            Some(parallel),
            Some(!decompress)).await.unwrap();

        // Map labels (morton codes) back to grid coordinates
        let label_to_coord: HashMap<u64, UnboundedGridCoord> =
            morton_codes.iter().enumerate().map(|(i, label)| (*label, grid_coords.get(i).unwrap().clone())).collect();
        return optimized_bundles.into_iter().map(|bundle| {
            bundle.iter().map(|label| label_to_coord.get(label).unwrap().clone()).collect()
        }).collect();
    }

    async fn get_block_data<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_coords: Vec<UnboundedGridCoord>,
    ) -> Vec<Option<(VecDataBlock<T>, Option<String>)>>
    where
        VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
        T: ReflectedType,
    {
        let da2 = data_attrs.clone();

        // FIXME: Input should be bounded already
        let bounded_grid_coords = grid_coords.iter().map(|coords| GridCoord::from_vec(coords.iter().map(|x| *x as u64).collect())).collect();

        // TODO: Could be nicer
        let zoom_level = data_attrs.get_scales().iter().position(|s| s.key == path_name).unwrap();
        let voxel_offset = data_attrs.get_voxel_offset(zoom_level);
        let chunk_size = data_attrs.get_block_size(zoom_level);
        let dimensions = data_attrs.get_dimensions(zoom_level);

        let loader = HTTPDataLoader {};

        // Handling sharding is handled here, because
        if data_attrs.is_sharded(zoom_level) {
            let meta = ngpre::PrecomputedMetadata {
                cloudpath: self.base_path.to_string(),
            };

            //full_bbox = requested_bbox.expand_to_chunk_size(
            //  meta.chunk_size(mip), offset=meta.voxel_offset(mip)
            //)
            //full_bbox = Bbox.clamp(full_bbox, meta.bounds(mip))
            // We only read a single block here (for now)
            // let full_box = voxel_offset -> chunk_size

            // bounds(mip) is dataset size in voxels
            // grid_size = np.ceil(meta.bounds(mip).size3() / chunk_size).astype(np.uint32)
            let bounds = data_attrs.bounds(zoom_level);
            let grid_size: Vec<u64> = bounds[0].iter().zip(bounds[1].iter()).enumerate()
                .map(|(i, (bmin, bmax))| ((bmax - bmin) as u64).div_ceil(chunk_size[i].into()))
                .collect();
            //bounds = meta.bounds(mip)
            //gpts = list(gridpoints(full_bbox, bounds, chunk_size))
            //let gpts = gridpoints(full_bbox, bounds, chunk_size);
            //let gpts = vec![offset_grid_position.clone()];
            let gpts = bounded_grid_coords;
            let morton_codes = ngpre::compressed_morton_code(&gpts, &grid_size).unwrap();

            // Get raw data and send back
            let progress = false;
            let decompress = false;
            let parallel = 0;

            // The cache service ultimately retrieves the data. We need to provide it with a
            // data loader to download the data from the respective path/URL.
            let cache = ngpre::CacheService {
                enabled: false,
                data_loader: &loader,
                meta: &meta,
            };
            let spec = data_attrs.get_sharding_spec(zoom_level).unwrap();
            let mut reader = ngpre::ShardReader::new(&meta, &cache, &spec, &loader, None, None);

            let data_key = data_attrs.get_key(zoom_level);
            let io_chunkdata = reader.get_data(
                &morton_codes,
                Some(data_key),
                Some(progress),
                Some(parallel),
                Some(!decompress)).await.unwrap();

            let block_data = io_chunkdata.iter().map(|(&morton_code, io_data)| {
                assert!(io_data.is_some());
                let (buff, etag) = io_data.as_ref().unwrap();
                let img_data = decode(&buff);

                let offset_grid_position = gpts[morton_codes.iter().position(|&r| r == morton_code).unwrap()].clone();

                return Some((<ngpre::DefaultBlock as ngpre::DefaultBlockReader<T, &[u8]>>::read_block(
                    &img_data,
                    &da2,
                    offset_grid_position,
                    zoom_level).unwrap(),
                    etag.clone()));
            }).collect();

            return block_data;
        } else {
            let loaded_data = loader.get(
                self.base_path.to_string(),
                Some(false),
                &grid_coords.iter().filter(|grid_coord| {
                    // Make sure we are in bounds with requested blocks. This method accepts signed inputed, to
                    // allow catching overflows when data from JavaScript is passed in.
                    let mut n = 0;
                    for coord in grid_coord.iter() {
                        if *coord < 0 || *coord * chunk_size[n] as i64 > dimensions[n] as i64  {
                            return false;
                        }
                        n += 1;
                    }
                    return true
                }).map(|grid_coord| {
                    (
                        self.relative_block_path(path_name, &grid_coord, chunk_size, voxel_offset, dimensions),
                        grid_coord.clone()
                    )
                }).collect()).await;

            let block_data: Vec<_> = loaded_data.iter().map(|((_path, grid_coord), result)| {
                return match result {
                    Ok(block_details) => {
                        Some((<ngpre::DefaultBlock as ngpre::DefaultBlockReader<T, &[u8]>>::read_block(
                            &block_details.content,
                            &da2,
                            GridCoord::from(grid_coord.iter().map(|c| *c as u64).collect::<Vec<_>>()),
                            zoom_level).unwrap(),
                            block_details.etag.clone()))
                    },
                    Err(_why) => {
                        None
                    }
                };
            }).collect();

            return block_data;
        }
    }
}

#[wasm_bindgen]
impl NgPreHTTPFetch {
    pub async fn open(base_path: &str) -> Result<JsValue, JsValue> {
        utils::set_panic_hook();

        let reader = NgPreHTTPFetch {
            base_path: base_path.into(),
        };

        let version = NgPreAsyncReader::get_version(&reader).await;
        if !ngpre::is_version_compatible(&ngpre::VERSION, &version) {
            return Err(JsValue::from("TODO: Incompatible version"));
        };

        return Ok(JsValue::from(reader))
    }

    pub async fn get_version(&self) -> JsValue {
        NgPrePromiseReader::get_version(self).await
    }

    pub async fn get_dataset_attributes(&self, path_name: &str) -> JsValue {
        NgPrePromiseReader::get_dataset_attributes(self, path_name).await
    }

    pub async fn exists(&self, path_name: &str) -> bool {
        NgPrePromiseReader::exists(self, path_name).await
    }

    pub async fn dataset_exists(&self, path_name: &str) -> bool {
        NgPrePromiseReader::dataset_exists(self, path_name).await
    }

    pub async fn read_block(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue {
        NgPrePromiseReader::read_block(self, path_name, data_attrs, grid_position).await
    }

    pub async fn list_attributes(&self, path_name: &str) -> JsValue {
        NgPrePromiseReader::list_attributes(self, path_name).await
    }

    pub async fn block_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue {
        NgPrePromiseEtagReader::block_etag(
            self, path_name, data_attrs, grid_position).await
    }

    pub async fn read_block_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue {
        NgPrePromiseEtagReader::read_block_with_etag(self, path_name, data_attrs, grid_position).await
    }

    pub async fn read_blocks_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        flattened_grid_coords: Vec<i64>,
    ) -> Box<[JsValue]> {
        NgPrePromiseEtagReader::read_blocks_with_etag(self, path_name, data_attrs, flattened_grid_coords).await
    }

    pub async fn get_optimized_request_bundles(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        flattened_grid_coords: Vec<i64>,
    ) -> Box<[JsValue]> {
        NgPrePromiseEtagReader::get_optimized_request_bundles(self, path_name, data_attrs, flattened_grid_coords).await
    }
}

struct HTTPDataLoader {}

fn path_join(paths: Vec<&str>) -> Option<String> {
    let mut path = PathBuf::new();
    path.extend(paths);
    Some(path.to_str().unwrap().to_owned())
}

#[async_trait(?Send)]
impl DataLoader for HTTPDataLoader {

    async fn get(&self, base_path: String, _progress: Option<bool>, tuples: &Vec<(String, UnboundedGridCoord)>)
        -> HashMap<(String, UnboundedGridCoord), io::Result<DataLoaderResult>> {

        if tuples.len() == 0 {
            return HashMap::new();
        }

        let mut result = HashMap::new();
        let mut tasks = Vec::new();
        for (path, _grid_coord) in tuples.iter() {
            let full_path = path_join(vec![&base_path, &path]).unwrap();

            let request_options = RequestInit::new();
            request_options.set_method("GET");
            // CORS mode is needed for range requests
            request_options.set_mode(RequestMode::Cors);

            let req = Request::new_with_str_and_init(
                &full_path,
                &request_options).unwrap();

            tasks.push(JsFuture::from(self_().unwrap().fetch_with_request(&req)));
        }

        let task_results = join_all(tasks).await;
        for (i, response) in task_results.into_iter().enumerate() {
            let (path, grid_coord) = &tuples[i];

            let res = response.unwrap();
            assert!(res.is_instance_of::<Response>());
            let resp: Response = res.dyn_into().unwrap();

            let status = resp.status_text();
            if resp.ok() {
                let arrbuff = JsFuture::from(resp.array_buffer().unwrap()).await;
                match arrbuff {
                    Err(_why) => {
                        result.insert(
                            (path.clone(), grid_coord.clone()),
                            Err(io::Error::new(io::ErrorKind::InvalidInput, status))
                        );
                    },
                    Ok(arrbuff_value) => {
                        let typebuff: js_sys::Uint8Array = js_sys::Uint8Array::new(&arrbuff_value);
                        let buff = typebuff.to_vec();
                        result.insert(
                            (path.clone(), grid_coord.clone()),
                            Ok(DataLoaderResult {
                                path: path.clone(),
                                byterange: Range { start: 0, end: buff.len() as u64},
                                content: buff,
                                compress: "".to_string(),
                                raw: true,
                                etag: resp.headers().get("ETag").unwrap_or(None)
                            })
                        );
                    }
                };
            } else {
                result.insert(
                        (path.clone(), grid_coord.clone()),
                        Err(io::Error::new(io::ErrorKind::InvalidInput, status))
                    );
            }
        }

        result
    }

    async fn get_sharded(&self, base_path: String, _progress: Option<bool>, tuples: &Vec<(String, u64, u64)>)
        -> HashMap<(String, u64, u64), io::Result<DataLoaderResult>> {

        let mut result = HashMap::new();
        let mut tasks = Vec::new();
        for (path, byte_start, byte_end) in tuples.iter() {
            let full_path = path_join(vec![&base_path, &path]).unwrap();

            let request_options = RequestInit::new();
            request_options.set_method("GET");
            // CORS mode is needed for range requests.
            request_options.set_mode(RequestMode::Cors);

            let req = Request::new_with_str_and_init(
                &full_path,
                &request_options).unwrap();

            let _ = req.headers().set("Range",
                &format!("bytes={}-{}", byte_start, byte_end - 1));

            tasks.push(JsFuture::from(self_().unwrap().fetch_with_request(&req)));
        }

        let task_results = join_all(tasks).await;
        for (i, response) in task_results.into_iter().enumerate() {
            let (path, byte_start, byte_end) = &tuples[i];

            let res = response.unwrap();
            assert!(res.is_instance_of::<Response>());
            let resp: Response = res.dyn_into().unwrap();

            let status = resp.status_text();
            if resp.ok() {
                let arrbuff = JsFuture::from(resp.array_buffer().unwrap()).await;
                match arrbuff {
                    Err(_why) => {
                        result.insert(
                            (path.clone(), byte_start.clone(), byte_end.clone()),
                            Err(io::Error::new(io::ErrorKind::InvalidInput, status))
                        );
                    },
                    Ok(arrbuff_value) => {
                        let typebuff: js_sys::Uint8Array = js_sys::Uint8Array::new(&arrbuff_value);
                        let buff = typebuff.to_vec();
                        result.insert(
                            (path.clone(), byte_start.clone(), byte_end.clone()),
                            Ok(DataLoaderResult {
                                path: path.clone(),
                                byterange: Range { start: byte_start.clone(), end: byte_end.clone() },
                                content: buff,
                                compress: "".to_string(),
                                raw: true,
                                etag: resp.headers().get("ETag").unwrap_or(None)
                            })
                        );
                    }
                };
            } else {
                result.insert(
                        (path.clone(), byte_start.clone(), byte_end.clone()),
                        Err(io::Error::new(io::ErrorKind::InvalidInput, status))
                    );
            }
        }

        result
    }
}

impl NgPreAsyncReader for NgPreHTTPFetch {
    async fn get_version(&self) -> ngpre::Version {
        ngpre::Version::from_str(&"2.3.0").unwrap()
    }

    async fn get_dataset_attributes(&self, path_name: &str) -> ngpre::DatasetAttributes {
        utils::set_panic_hook();

        let path = self.get_dataset_attributes_path(path_name);
        let js_val = self.fetch_json(&path, true).await;
        serde_wasm_bindgen::from_value(js_val).unwrap()
    }

    async fn exists(&self, path_name: &str) -> bool {
        let resp_value = self.fetch(path_name, true).await.unwrap();
        assert!(resp_value.is_instance_of::<Response>());
        let resp: Response = resp_value.dyn_into().unwrap();
        resp.ok()
    }

    // Override the default NgPreAsyncReader impl to not require the GET on the
    // dataset directory path to be 200.
    async fn dataset_exists(&self, path_name: &str) -> bool {
        let path = self.get_dataset_attributes_path(path_name);
        NgPreAsyncReader::exists(self, &path).await
    }

    async fn read_block<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<VecDataBlock<T>>
    where
        VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
        T: ReflectedType,
    {
        NgPreAsyncEtagReader::read_block_with_etag(self, path_name, data_attrs, grid_position)
            .await.map(|maybe_block| maybe_block.0)
    }

    async fn list(&self, _path_name: &str) -> Vec<String> {
        // TODO: Not implemented because remote paths are not listable.
        unimplemented!()
    }

    async fn list_attributes(
        &self,
        path_name: &str,
    ) -> serde_json::Value {
        self.get_attributes(path_name).await
    }
}

impl NgPreAsyncEtagReader for NgPreHTTPFetch {
    async fn block_etag(
        &self,
        path_name: &str,
        _data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<String> {
        let request_options = RequestInit::new();
        request_options.set_method("HEAD");
        request_options.set_mode(RequestMode::Cors);

        // TODO: Could be nicer
        let zoom_level = _data_attrs.get_scales().iter().position(|s| s.key == path_name).unwrap();

        let block_path = self.relative_block_path(path_name, &grid_position,
                 _data_attrs.get_block_size(zoom_level), _data_attrs.get_voxel_offset(zoom_level),
                 _data_attrs.get_dimensions(zoom_level));

        let req = Request::new_with_str_and_init(
            &format!("{}/{}", &self.base_path, block_path),
            &request_options).unwrap();
        let req_promise = self_().unwrap().fetch_with_request(&req);

        JsFuture::from(req_promise)
            .map_ok(|resp_value| {
                assert!(resp_value.is_instance_of::<Response>());
                let resp: Response = resp_value.dyn_into().unwrap();

                if resp.ok() {
                    resp.headers().get("ETag").unwrap_or(None)
                } else {
                    None
                }
            }).await.unwrap()
    }

    async fn read_block_with_etag<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<(VecDataBlock<T>, Option<String>)>
    where
        VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
        T: ReflectedType,
    {
        let performance = self_().unwrap().performance();

        let grid_pos = grid_position.clone();
        let start = perf_to_system(performance.now());

        let mut block_data = self.get_block_data(path_name, data_attrs, vec![grid_position.clone()]).await;
        let block = if block_data.len() == 0 { None } else { block_data.remove(0) };

        let end = perf_to_system(performance.now());
        console::log_1(&format!("read_block_with_etag {:?} duration: {:?}ms", &grid_pos,
                end.duration_since(start).unwrap().as_millis()).into());

        return block;
    }

    async fn read_blocks_with_etag<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_coords: Vec<UnboundedGridCoord>,
    ) -> Vec<Option<(VecDataBlock<T>, Option<String>)>>
    where
        VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
        T: ReflectedType,
    {
        let performance = self_().unwrap().performance();
        let start = perf_to_system(performance.now());

        let block_data = self.get_block_data(path_name, data_attrs, grid_coords.clone()).await;

        let end = perf_to_system(performance.now());
        console::log_1(&format!("read_blocks_with_etag of {:?} blocks, duration: {:?}ms", &grid_coords.len(),
                end.duration_since(start).unwrap().as_millis()).into());

        return block_data;
    }

    async fn get_optimized_request_bundles(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_coords: Vec<UnboundedGridCoord>,
    ) -> Vec<Vec<UnboundedGridCoord>>
    {
        let performance = self_().unwrap().performance();
        let start = perf_to_system(performance.now());

        let bundles = self.optimize_requests::<u8>(path_name, data_attrs, grid_coords.clone()).await;

        let end = perf_to_system(performance.now());
        console::log_1(&format!("get_optimized_request_bundles of {:?} blocks, duration: {:?}ms", &grid_coords.len(),
                end.duration_since(start).unwrap().as_millis()).into());

        bundles
    }
}
