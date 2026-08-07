//! Adapters wrapping frames so they can be fed to the safety check.
//!
//! Mirrors the Go proxy's `GlobalFrameWrapper`: a frame's identity is the
//! Poseidon hash of its output (`poseidon(header.output)`), and its parent
//! selector is the stored `header.parent_selector` (which the proposer computed
//! as the Poseidon hash of the parent frame's output). A correct chain therefore
//! has `child.parent_selector == parent.identity`. App-shard frames use the
//! exact same linkage model (their output is the deterministic app frame
//! output rather than a VDF, but the selector arithmetic is identical), so
//! [`AppShardFrameWrapper`] lets `safety::check_safety` run over a polled
//! app-shard chain unchanged.

use anyhow::{anyhow, bail};
use quil_types::proto::global::{AppShardFrame, GlobalFrame};

use crate::safety::FrameFields;

/// Lets a boxed frame be fed to `safety::check_safety` unchanged, so the frame
/// fetcher can return one erased `Vec` for either chain instead of duplicating
/// its loop per concrete wrapper type.
impl FrameFields for Box<dyn FrameFields> {
    fn identity(&self) -> anyhow::Result<[u8; 32]> {
        (**self).identity()
    }

    fn parent_selector(&self) -> anyhow::Result<[u8; 32]> {
        (**self).parent_selector()
    }
}

/// Wraps a `GlobalFrame` to expose its 32-byte identity and parent selector.
#[derive(Debug, Clone)]
pub struct GlobalFrameWrapper {
    pub frame: GlobalFrame,
}

impl GlobalFrameWrapper {
    pub fn new(frame: GlobalFrame) -> Self {
        Self { frame }
    }
}

impl FrameFields for GlobalFrameWrapper {
    fn identity(&self) -> anyhow::Result<[u8; 32]> {
        let header = self
            .frame
            .header
            .as_ref()
            .ok_or_else(|| anyhow!("frame header is nil"))?;
        quil_crypto::poseidon::hash_bytes_to_32(&header.output)
            .map_err(|e| anyhow!("compute frame identity (poseidon): {e}"))
    }

    fn parent_selector(&self) -> anyhow::Result<[u8; 32]> {
        let header = self
            .frame
            .header
            .as_ref()
            .ok_or_else(|| anyhow!("frame header is nil"))?;
        if header.parent_selector.len() != 32 {
            bail!(
                "parent selector is not 32 bytes: got {}",
                header.parent_selector.len()
            );
        }
        Ok(header.parent_selector.clone().try_into().unwrap())
    }
}

/// Wraps an `AppShardFrame` to expose its 32-byte identity and parent selector.
/// The identity model matches the master's own mirror staging (selector =
/// `poseidon(header.output)`, `worker_manager.rs` `FullFrameProduced`).
#[derive(Debug, Clone)]
pub struct AppShardFrameWrapper {
    pub frame: AppShardFrame,
}

impl AppShardFrameWrapper {
    pub fn new(frame: AppShardFrame) -> Self {
        Self { frame }
    }
}

impl FrameFields for AppShardFrameWrapper {
    fn identity(&self) -> anyhow::Result<[u8; 32]> {
        let header = self
            .frame
            .header
            .as_ref()
            .ok_or_else(|| anyhow!("app shard frame header is nil"))?;
        quil_crypto::poseidon::hash_bytes_to_32(&header.output)
            .map_err(|e| anyhow!("compute app shard frame identity (poseidon): {e}"))
    }

    fn parent_selector(&self) -> anyhow::Result<[u8; 32]> {
        let header = self
            .frame
            .header
            .as_ref()
            .ok_or_else(|| anyhow!("app shard frame header is nil"))?;
        if header.parent_selector.len() != 32 {
            bail!(
                "app shard parent selector is not 32 bytes: got {}",
                header.parent_selector.len()
            );
        }
        Ok(header.parent_selector.clone().try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::safety::{check_safety, SafetyError};
    use quil_types::proto::global::{FrameHeader, GlobalFrameHeader};

    fn header_with_output(output: Vec<u8>, parent_selector: Vec<u8>) -> GlobalFrameHeader {
        GlobalFrameHeader {
            frame_number: 0,
            rank: 0,
            timestamp: 0,
            difficulty: 0,
            output,
            parent_selector,
            global_commitments: Vec::new(),
            prover_tree_commitment: Vec::new(),
            requests_root: Vec::new(),
            prover: Vec::new(),
            ..Default::default()
        }
    }

    fn frame(output: Vec<u8>, parent_selector: Vec<u8>) -> GlobalFrameWrapper {
        GlobalFrameWrapper::new(GlobalFrame {
            header: Some(header_with_output(output, parent_selector)),
            ..Default::default()
        })
    }

    #[test]
    fn identity_is_poseidon_of_output() {
        let output = vec![0xABu8; 516];
        let expected = quil_crypto::poseidon::hash_bytes_to_32(&output).unwrap();
        let w = frame(output, vec![0u8; 32]);
        assert_eq!(w.identity().unwrap(), expected);
    }

    #[test]
    fn nil_header_errors() {
        let w = GlobalFrameWrapper::new(GlobalFrame {
            header: None,
            ..Default::default()
        });
        assert!(w.identity().is_err());
        assert!(w.parent_selector().is_err());
    }

    #[test]
    fn wrong_parent_selector_length_errors() {
        let w = frame(vec![1u8; 516], vec![0u8; 16]);
        assert!(w.parent_selector().is_err());
    }

    #[test]
    fn poseidon_linked_chain_passes_safety() {
        // Build a 4-frame chain where each frame's parent_selector is the
        // poseidon identity of the previous frame's output — exactly how a
        // correct global chain links — and confirm the safety check accepts it.
        let outputs: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i + 1; 516]).collect();
        let mut frames = Vec::new();
        // Root: arbitrary parent selector (no parent in the set).
        frames.push(frame(outputs[0].clone(), vec![0u8; 32]));
        for i in 1..4 {
            let parent_id = quil_crypto::poseidon::hash_bytes_to_32(&outputs[i - 1]).unwrap();
            frames.push(frame(outputs[i].clone(), parent_id.to_vec()));
        }
        check_safety(&frames).expect("a correctly poseidon-linked chain is safe");
    }

    #[test]
    fn forked_chain_fails_safety() {
        // Two frames share the same parent → fork.
        let root_out = vec![9u8; 516];
        let root_id = quil_crypto::poseidon::hash_bytes_to_32(&root_out).unwrap();
        let frames = vec![
            frame(root_out, vec![0u8; 32]),
            frame(vec![10u8; 516], root_id.to_vec()),
            frame(vec![11u8; 516], root_id.to_vec()),
        ];
        assert!(matches!(check_safety(&frames), Err(SafetyError::Fork(_))));
    }

    fn app_frame(output: Vec<u8>, parent_selector: Vec<u8>) -> AppShardFrameWrapper {
        AppShardFrameWrapper::new(AppShardFrame {
            header: Some(FrameHeader {
                output,
                parent_selector,
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    #[test]
    fn app_identity_is_poseidon_of_output() {
        let output = vec![0xCDu8; 128];
        let expected = quil_crypto::poseidon::hash_bytes_to_32(&output).unwrap();
        let w = app_frame(output, vec![0u8; 32]);
        assert_eq!(w.identity().unwrap(), expected);
    }

    #[test]
    fn app_nil_header_errors() {
        let w = AppShardFrameWrapper::new(AppShardFrame {
            header: None,
            ..Default::default()
        });
        assert!(w.identity().is_err());
        assert!(w.parent_selector().is_err());
    }

    #[test]
    fn app_poseidon_linked_chain_passes_safety() {
        // Same linkage as the global chain: each frame's parent_selector is
        // the poseidon identity of the previous frame's output. The root's
        // parent (the shard genesis frame) is absent from the fetched range
        // `1..=stop_frame`, which the safety check tolerates.
        let outputs: Vec<Vec<u8>> = (0..3u8).map(|i| vec![i + 1; 128]).collect();
        let mut frames = Vec::new();
        frames.push(app_frame(outputs[0].clone(), vec![7u8; 32]));
        for i in 1..3 {
            let parent_id = quil_crypto::poseidon::hash_bytes_to_32(&outputs[i - 1]).unwrap();
            frames.push(app_frame(outputs[i].clone(), parent_id.to_vec()));
        }
        check_safety(&frames).expect("a correctly poseidon-linked app chain is safe");
    }

    #[test]
    fn app_forked_chain_fails_safety() {
        let root_out = vec![9u8; 128];
        let root_id = quil_crypto::poseidon::hash_bytes_to_32(&root_out).unwrap();
        let frames = vec![
            app_frame(root_out, vec![7u8; 32]),
            app_frame(vec![10u8; 128], root_id.to_vec()),
            app_frame(vec![11u8; 128], root_id.to_vec()),
        ];
        assert!(matches!(check_safety(&frames), Err(SafetyError::Fork(_))));
    }
}
