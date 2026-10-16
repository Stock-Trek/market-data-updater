use parquet::{basic::Compression, file::properties::WriterProperties};

pub fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build()
}
