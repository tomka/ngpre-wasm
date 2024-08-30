use futures;
use js_sys;
use ngpre;
use serde_json;
use wasm_bindgen;
use wasm_bindgen_futures;
use web_sys;

mod utils;

use std::io::{
    Error,
    ErrorKind,
};

use js_sys::Promise;
use futures::{future, Future};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use ngpre::prelude::*;
use ngpre::{data_type_match, data_type_rstype_replace};


pub mod http_fetch;

pub trait NgPrePromiseReader {
    /// Get the NgPre specification version of the container.
    fn get_version(&self) -> Promise;

    fn get_dataset_attributes(&self, path_name: &str) -> Promise;

    fn exists(&self, path_name: &str) -> Promise;

    fn dataset_exists(&self, path_name: &str) -> Promise;

    fn read_block(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise;

    fn list_attributes(&self, path_name: &str) -> Promise;
}

impl<T> NgPrePromiseReader for T where T: NgPreAsyncReader {
    fn get_version(&self) -> Promise {
        utils::set_panic_hook();
        let to_return = self.get_version()
            .map(|v| JsValue::from(wrapped::Version(v)));

        future_to_promise(map_future_error_wasm(to_return))
    }

    fn get_dataset_attributes(&self, path_name: &str) -> Promise {
        let to_return = self.get_dataset_attributes(path_name)
            .map(|da| JsValue::from(wrapped::DatasetAttributes(da)));

        future_to_promise(map_future_error_wasm(to_return))
    }

    fn exists(&self, path_name: &str) -> Promise {
        let to_return = self.exists(path_name)
            .map(JsValue::from);

        future_to_promise(map_future_error_wasm(to_return))
    }

    fn dataset_exists(&self, path_name: &str) -> Promise {
        let to_return = self.dataset_exists(path_name)
            .map(JsValue::from);

        future_to_promise(map_future_error_wasm(to_return))
    }

    fn read_block(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise {

        data_type_match! {
            data_attrs.0.get_data_type(),
            future_to_promise(map_future_error_wasm(
                self.read_block::<RsType>(path_name, &data_attrs.0, grid_position.into())
                    .map(|maybe_block| JsValue::from(
                        maybe_block.map(<RsType as VecBlockMonomorphizerReflection>::MONOMORPH::from)))))
        }
    }

    fn list_attributes(
        &self,
        path_name: &str,
    ) -> Promise {

        // TODO: Superfluous conversion from JSON to JsValue to serde to JsValue.
        let to_return = self.list_attributes(path_name)
            .map(|v| serde_wasm_bindgen::to_value(&v).unwrap());

        future_to_promise(map_future_error_wasm(to_return))
    }
}


pub trait NgPrePromiseEtagReader {
    fn block_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise;

    fn read_block_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise;
}

impl<T> NgPrePromiseEtagReader for T where T: NgPreAsyncEtagReader {
    fn block_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise {
        let to_return = self.block_etag(path_name, &data_attrs.0, grid_position.into())
            .map(JsValue::from);

        future_to_promise(map_future_error_wasm(to_return))
    }

    fn read_block_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> Promise {

        data_type_match! {
            data_attrs.0.get_data_type(),
            future_to_promise(map_future_error_wasm(
                self.read_block_with_etag::<RsType>(path_name, &data_attrs.0, grid_position.into())
                    .map(|maybe_block| JsValue::from(
                        maybe_block.map(<RsType as VecBlockMonomorphizerReflection>::MONOMORPH::from)))))
        }
    }
}


/// This trait exists to preserve type information between calls (rather than
/// erasing it with `Promise`) and for easier potential future compatibility
/// with an NgPre core async trait.
pub trait NgPreAsyncReader {
    fn get_version(&self) -> Box<dyn Future<Item = ngpre::Version, Error = Error>>;

    fn get_dataset_attributes(&self, path_name: &str) ->
        Box<dyn Future<Item = ngpre::DatasetAttributes, Error = Error>>;

    fn exists(&self, path_name: &str) -> Box<dyn Future<Item = bool, Error = Error>>;

    fn dataset_exists(&self, path_name: &str) -> Box<dyn Future<Item = bool, Error = Error>> {
        Box::new(self.exists(path_name).join(
            self.get_dataset_attributes(path_name)
                .map(|_| true)
                .or_else(|_| futures::future::ok(false))
        ).map(|(exists, has_attr)| exists && has_attr))
    }

    fn read_block<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Box<dyn Future<Item = Option<VecDataBlock<T>>, Error = Error>>
            where VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
                T: ReflectedType;

    fn list(&self, path_name: &str) -> Box<dyn Future<Item = Vec<String>, Error = Error>>;

    fn list_attributes(&self, path_name: &str) -> Box<dyn Future<Item = serde_json::Value, Error = Error>>;
}


pub trait NgPreAsyncEtagReader {
    fn block_etag(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Box<dyn Future<Item = Option<String>, Error = Error>>;

    fn read_block_with_etag<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Box<dyn Future<Item = Option<(VecDataBlock<T>, Option<String>)>, Error = Error>>
            where VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
                T: ReflectedType;
}


fn map_future_error_rust<F: Future<Item = T, Error = JsValue>, T>(future: F)
        -> impl Future<Item = T, Error = Error> {
    future.map_err(convert_jsvalue_error)
}

fn map_future_error_wasm<F: Future<Item = T, Error = Error>, T>(future: F)
        -> impl Future<Item = T, Error = JsValue> {
    future.map_err(|error| {
        let js_error = js_sys::Error::new(&format!("{:?}", error));
        JsValue::from(js_error)
    })
}

fn convert_jsvalue_error(error: JsValue) -> Error {
    Error::new(std::io::ErrorKind::Other, format!("{:?}", error))
}


pub mod wrapped {
    use super::*;

    #[wasm_bindgen]
    pub struct Version(pub(crate) ngpre::Version);

    #[wasm_bindgen]
    impl Version {
        pub fn to_string(&self) -> String {
            self.0.to_string()
        }
    }

    #[wasm_bindgen]
    #[derive(serde::Deserialize, serde::Serialize)]
    pub struct DatasetAttributes(pub(crate) ngpre::DatasetAttributes);

    #[wasm_bindgen]
    impl DatasetAttributes {
        pub fn get_dimensions(&self, zoom_level: usize) -> Vec<u64> {
            self.0.get_dimensions(zoom_level).to_owned()
        }

        pub fn get_block_size(&self, zoom_level: usize) -> Vec<u32> {
            self.0.get_block_size(zoom_level).to_owned()
        }

        pub fn get_voxel_offset(&self, zoom_level: usize) -> Vec<i64> {
            self.0.get_voxel_offset(zoom_level).to_owned()
        }

        pub fn get_data_type(&self) -> String {
            self.0.get_data_type().to_string()
        }

        pub fn get_compression(&self, zoom_level: usize) -> String {
            self.0.get_compression(zoom_level).to_string()
        }

        pub fn get_ndim(&self, zoom_level: usize) -> usize {
            self.0.get_ndim(zoom_level)
        }

        /// Get the total number of elements possible given the dimensions.
        pub fn get_num_elements(&self, zoom_level: usize) -> usize {
            self.0.get_num_elements(zoom_level)
        }

        /// Get the total number of elements possible in a block.
        pub fn get_block_num_elements(&self, zoom_level: usize) -> usize {
            self.0.get_block_num_elements(zoom_level)
        }

        pub fn to_json(&self) -> JsValue {
            serde_wasm_bindgen::to_value(&self).unwrap()
        }

        pub fn from_json(js: &JsValue) -> Self {
            serde_wasm_bindgen::from_value(js.clone()).unwrap()
        }
    }
}

trait VecBlockMonomorphizerReflection {
    type MONOMORPH;
}

macro_rules! data_block_monomorphizer {
    ($d_name:ident, $d_type:ty) => {
        #[wasm_bindgen]
        pub struct $d_name(VecDataBlock<$d_type>, Option<String>);

        impl VecBlockMonomorphizerReflection for $d_type {
            type MONOMORPH = $d_name;
        }

        impl From<VecDataBlock<$d_type>> for $d_name {
            fn from(block: VecDataBlock<$d_type>) -> Self {
                $d_name(block, None)
            }
        }

        impl From<(VecDataBlock<$d_type>, Option<String>)> for $d_name {
            fn from((block, etag): (VecDataBlock<$d_type>, Option<String>)) -> Self {
                $d_name(block, etag)
            }
        }

        #[wasm_bindgen]
        impl $d_name {
            pub fn get_size(&self) -> Vec<u32> {
                self.0.get_size().to_owned()
            }

            pub fn get_grid_position(&self) -> Vec<u64> {
                self.0.get_grid_position().to_owned()
            }

            pub fn get_data(&self) -> Vec<$d_type> {
                self.0.get_data().to_owned()
            }

            pub fn into_data(self) -> Vec<$d_type> {
                self.0.into_data()
            }

            pub fn get_num_elements(&self) -> u32 {
                self.0.get_num_elements()
            }

            pub fn get_etag(&self) -> Option<String> {
                self.1.to_owned()
            }
        }
    }
}

data_block_monomorphizer!(VecDataBlockUINT8,  u8);
data_block_monomorphizer!(VecDataBlockUINT16, u16);
data_block_monomorphizer!(VecDataBlockUINT32, u32);
data_block_monomorphizer!(VecDataBlockUINT64, u64);
data_block_monomorphizer!(VecDataBlockINT8,  i8);
data_block_monomorphizer!(VecDataBlockINT16, i16);
data_block_monomorphizer!(VecDataBlockINT32, i32);
data_block_monomorphizer!(VecDataBlockINT64, i64);
data_block_monomorphizer!(VecDataBlockFLOAT32, f32);
data_block_monomorphizer!(VecDataBlockFLOAT64, f64);
