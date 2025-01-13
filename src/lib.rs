use futures::{self};
use js_sys;
use serde_json;
use wasm_bindgen;
use wasm_bindgen_futures;
use web_sys;

pub mod http_fetch;
mod utils;

use js_sys::Promise;
use wasm_bindgen::prelude::*;

use ngpre::prelude::*;
use ngpre::{data_type_match, data_type_rstype_replace};

#[allow(async_fn_in_trait)]
pub trait NgPrePromiseReader {
    /// Get the NgPre specification version of the container.
    async fn get_version(&self) -> JsValue;

    async fn get_dataset_attributes(&self, path_name: &str) -> JsValue;

    async fn exists(&self, path_name: &str) -> bool;

    async fn dataset_exists(&self, path_name: &str) -> bool;

    async fn read_block(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue;

    async fn list_attributes(&self, path_name: &str) -> JsValue;
}

impl<T> NgPrePromiseReader for T where T: NgPreAsyncReader {
    async fn get_version(&self) -> JsValue {
        utils::set_panic_hook();
        let ver = self.get_version().await;
        JsValue::from(wrapped::Version(ver))
    }

    async fn get_dataset_attributes(&self, path_name: &str) -> JsValue {
        let attrs = self.get_dataset_attributes(path_name).await;
        JsValue::from(wrapped::DatasetAttributes(attrs))
    }

    async fn exists(&self, path_name: &str) -> bool {
        self.exists(path_name).await
    }

    async fn dataset_exists(&self, path_name: &str) -> bool {
        self.dataset_exists(path_name).await
    }

    async fn read_block(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue {
        // Have to clone the value returned by `read_block_with_etag` because `self` escapes the
        // function scope.
        data_type_match! {
            data_attrs.0.get_data_type(),
            self.read_block::<RsType>(path_name, &data_attrs.0, grid_position.into()).await.map(|val| {
                JsValue::from(<RsType as VecBlockMonomorphizerReflection>::MONOMORPH::from(val))
            }).unwrap_or(JsValue::NULL)
        }
    }

    async fn list_attributes(
        &self,
        path_name: &str,
    ) -> JsValue {
        // TODO: Superfluous conversion from JSON to JsValue to serde to JsValue.
        let list_attrs = self.list_attributes(path_name).await;
        serde_wasm_bindgen::to_value(&list_attrs).unwrap()
    }
}


#[allow(async_fn_in_trait)]
pub trait NgPrePromiseEtagReader {
    async fn block_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue;

    async fn read_block_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue;

    async fn read_blocks_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        flattened_grid_coords: Vec<i64>,
    ) -> Box<[JsValue]>;
}

impl<T> NgPrePromiseEtagReader for T where T: NgPreAsyncEtagReader {
    async fn block_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue {
        let etag = self.block_etag(path_name, &data_attrs.0, grid_position.into()).await;
        JsValue::from(etag.unwrap_or_default())
    }

    async fn read_block_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        grid_position: Vec<i64>,
    ) -> JsValue {
        data_type_match! {
            data_attrs.0.get_data_type(),
            self.read_block_with_etag::<RsType>(path_name, &data_attrs.0, grid_position.into()).await.map(|(val, etag)| {
                JsValue::from(<RsType as VecBlockMonomorphizerReflection>::MONOMORPH::from((val, etag)))
            }).unwrap_or(JsValue::NULL)
        }
    }

    async fn read_blocks_with_etag(
        &self,
        path_name: &str,
        data_attrs: &wrapped::DatasetAttributes,
        flattened_grid_coords: Vec<i64>,
    ) -> Box<[JsValue]> {
        data_type_match! {
            data_attrs.0.get_data_type(),
            self.read_blocks_with_etag::<RsType>(path_name, &data_attrs.0,
                    // FIXME: Avoid .clone()?
                    flattened_grid_coords.chunks(3).map(|c| c.to_vec().into()).collect()).await.iter().map(|result| {
                // FIXME: Avoid clone()
                result.clone().map(|(val, etag)| {
                    JsValue::from(<RsType as VecBlockMonomorphizerReflection>::MONOMORPH::from((val, etag)))
                }).unwrap_or(JsValue::NULL)
            }).collect::<Vec<_>>().into_boxed_slice()
        }
    }
}


/// This trait exists to preserve type information between calls (rather than
/// erasing it with `Promise`) and for easier potential future compatibility
/// with an NgPre core async trait.
#[allow(async_fn_in_trait)]
pub trait NgPreAsyncReader {
    async fn get_version(&self) -> ngpre::Version;

    async fn get_dataset_attributes(&self, path_name: &str) -> ngpre::DatasetAttributes;

    async fn exists(&self, path_name: &str) -> bool;

    // TODO: FIX ME
    async fn dataset_exists(&self, path_name: &str) -> bool {
        unimplemented!("what boolean value to return? {path_name}")
    }

    async fn read_block<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<VecDataBlock<T>>
            where VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
                T: ReflectedType;

    async fn list(&self, path_name: &str) -> Vec<String>;

    async fn list_attributes(&self, path_name: &str) -> serde_json::Value;
}


#[allow(async_fn_in_trait)]
pub trait NgPreAsyncEtagReader {
    async fn block_etag(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<String>;

    async fn read_block_with_etag<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_position: UnboundedGridCoord,
    ) -> Option<(VecDataBlock<T>, Option<String>)>
    where
        VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
        T: ReflectedType;

    async fn read_blocks_with_etag<T>(
        &self,
        path_name: &str,
        data_attrs: &DatasetAttributes,
        grid_coords: Vec<UnboundedGridCoord>,
    ) -> Vec<Option<(VecDataBlock<T>, Option<String>)>>
    where
        VecDataBlock<T>: DataBlock<T> + ngpre::ReadableDataBlock,
        T: ReflectedType;
}

pub mod wrapped {
    use std::fmt;

    use super::*;

    #[wasm_bindgen]
    pub struct Version(pub(crate) ngpre::Version);

    impl fmt::Display for Version {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
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

        pub fn get_voxel_offset(&self, zoom_level: usize) -> Vec<i32> {
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
