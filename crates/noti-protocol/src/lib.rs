#![allow(unsafe_code)]

//! # noti-protocol
//!
//! Generated protobuf / ConnectRPC types for the notification service.

pub mod noti {
    include!(concat!(env!("OUT_DIR"), "/_noti_include.rs"));
    pub use noti::*;
}
