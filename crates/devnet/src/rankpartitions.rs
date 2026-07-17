//! Generation of network-partition schedules for the devnet harness.
//!
//! A [`RankPartitionEntry`] describes a bipartite split of the node set to apply
//! when a specific consensus rank is observed over gossip. This module implements
//! the partition-schedule algorithm: parsing a schedule from JSON, enumerating
//! every bipartition, enumerating every complete per-rank schedule, and
//! deduplicating schedules that are equivalent under a *role-preserving*
//! permutation of node names (archives permute only with archives, clients only
//! with clients).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// A partition configuration to apply when a specific rank number is observed
/// over gossip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankPartitionEntry {
    pub rank: u64,
    pub partition1: Vec<String>,
    pub partition2: Vec<String>,
}

/// Parses a JSON-encoded list of [`RankPartitionEntry`] values and returns a
/// lookup map keyed by rank number. Rejects duplicate rank numbers and requires
/// all fields (`rank`, `partition1`, `partition2`) to be present in each entry.
pub fn parse_rank_partitions(raw: &str) -> anyhow::Result<BTreeMap<u64, RankPartitionEntry>> {
    // Decode to generic values first so we can enforce that every required field
    // is present (serde would otherwise happily default missing arrays).
    let raw_entries: Vec<serde_json::Value> =
        serde_json::from_str(raw).map_err(|e| anyhow::anyhow!("invalid JSON: {e}"))?;

    let mut m = BTreeMap::new();
    for (i, raw_entry) in raw_entries.iter().enumerate() {
        let obj = raw_entry
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("entry {i}: expected a JSON object"))?;
        for required in ["rank", "partition1", "partition2"] {
            if !obj.contains_key(required) {
                anyhow::bail!("entry {i}: missing required field {required:?}");
            }
        }
        let e: RankPartitionEntry = serde_json::from_value(raw_entry.clone())
            .map_err(|err| anyhow::anyhow!("entry {i}: {err}"))?;
        if m.contains_key(&e.rank) {
            anyhow::bail!("duplicate rank number {}", e.rank);
        }
        m.insert(e.rank, e);
    }
    Ok(m)
}

/// Enumerates every bipartite split of `nodes`. The first element of the sorted
/// node set is always placed in `partition1` to break the partition-swap
/// symmetry, so this returns `2^(n-1) - 1` splits (the all-ones case, which
/// would leave `partition2` empty, is excluded). Each split's two halves are
/// returned in sorted order.
fn all_bipartitions(nodes: &[String]) -> Vec<[Vec<String>; 2]> {
    if nodes.len() < 2 {
        return Vec::new();
    }
    let mut sorted = nodes.to_vec();
    sorted.sort();

    let rest = &sorted[1..];
    let total: usize = 1 << rest.len(); // 2^(n-1)
    let mut result = Vec::with_capacity(total - 1);

    for mask in 0..(total - 1) {
        // mask < total-1 excludes the all-ones case (p2 would be empty).
        let mut p1 = vec![sorted[0].clone()];
        let mut p2 = Vec::new();
        for (i, node) in rest.iter().enumerate() {
            if (mask >> i) & 1 == 1 {
                p1.push(node.clone());
            } else {
                p2.push(node.clone());
            }
        }
        result.push([p1, p2]);
    }
    result
}

/// Returns every possible complete partition schedule for `nodes` across ranks
/// `0..=stop_rank`. Each schedule is a list of [`RankPartitionEntry`] containing
/// one entry per rank that has a partition; ranks with no partition are omitted.
///
/// The schedules are produced by a mixed-radix counter over the ranks: each rank
/// independently takes one of `B + 1` values (0 = no partition, `1..=B` = which
/// bipartition), where `B` is the number of bipartitions.
fn all_rank_partitions(nodes: &[String], stop_rank: u64) -> Vec<Vec<RankPartitionEntry>> {
    let bipartitions = all_bipartitions(nodes);
    let b = bipartitions.len();
    let num_ranks = stop_rank as usize + 1;

    let mut out = Vec::new();
    let mut counter = vec![0usize; num_ranks];
    loop {
        let mut schedule = Vec::new();
        for (i, &digit) in counter.iter().enumerate() {
            if digit > 0 {
                let bp = &bipartitions[digit - 1];
                schedule.push(RankPartitionEntry {
                    rank: i as u64,
                    partition1: bp[0].clone(),
                    partition2: bp[1].clone(),
                });
            }
        }
        out.push(schedule);

        // Increment the mixed-radix counter (each digit in 0..=b).
        let mut pos = 0;
        while pos < counter.len() {
            counter[pos] += 1;
            if counter[pos] <= b {
                break;
            }
            counter[pos] = 0;
            pos += 1;
        }
        if pos == counter.len() {
            return out;
        }
    }
}

/// Generates every permutation of `nodes`.
fn generate_permutations(nodes: &[String]) -> Vec<Vec<String>> {
    if nodes.is_empty() {
        return vec![Vec::new()];
    }
    let mut result = Vec::new();
    for i in 0..nodes.len() {
        let mut rest = Vec::with_capacity(nodes.len() - 1);
        rest.extend_from_slice(&nodes[..i]);
        rest.extend_from_slice(&nodes[i + 1..]);
        for perm in generate_permutations(&rest) {
            let mut next = Vec::with_capacity(nodes.len());
            next.push(nodes[i].clone());
            next.extend(perm);
            result.push(next);
        }
    }
    result
}

/// Builds every *role-preserving* bijection of `nodes`: node names are permuted
/// only within their role class (an archive maps to an archive, a client to a
/// client). Each returned map sends every original name to its image under one
/// such bijection. This is the cartesian product of the within-archive and
/// within-client permutations — when every node shares a role it degenerates to
/// the full permutation group.
fn role_preserving_mappings(
    nodes: &[String],
    archives: &HashSet<String>,
) -> Vec<HashMap<String, String>> {
    let mut archive_names: Vec<String> = nodes
        .iter()
        .filter(|n| archives.contains(*n))
        .cloned()
        .collect();
    let mut client_names: Vec<String> = nodes
        .iter()
        .filter(|n| !archives.contains(*n))
        .cloned()
        .collect();
    archive_names.sort();
    client_names.sort();

    let archive_perms = generate_permutations(&archive_names);
    let client_perms = generate_permutations(&client_names);

    let mut result = Vec::with_capacity(archive_perms.len() * client_perms.len());
    for ap in &archive_perms {
        for cp in &client_perms {
            let mut mapping = HashMap::with_capacity(nodes.len());
            for (orig, img) in archive_names.iter().zip(ap.iter()) {
                mapping.insert(orig.clone(), img.clone());
            }
            for (orig, img) in client_names.iter().zip(cp.iter()) {
                mapping.insert(orig.clone(), img.clone());
            }
            result.push(mapping);
        }
    }
    result
}

/// Computes the lexicographically minimal string form of `schedule` over all
/// role-preserving permutations of `nodes`. Two schedules that are equal under
/// some role-preserving renaming produce the same canonical form, which is how
/// symmetry classes are detected.
fn canonical_form(
    schedule: &[RankPartitionEntry],
    nodes: &[String],
    archives: &HashSet<String>,
) -> String {
    let mappings = role_preserving_mappings(nodes, archives);
    let mut min_form: Option<String> = None;
    for mapping in &mappings {
        let mut sb = String::new();
        for entry in schedule {
            let mut p1: Vec<String> = entry
                .partition1
                .iter()
                .map(|n| mapping[n].clone())
                .collect();
            let mut p2: Vec<String> = entry
                .partition2
                .iter()
                .map(|n| mapping[n].clone())
                .collect();
            p1.sort();
            p2.sort();
            if p1[0] > p2[0] {
                std::mem::swap(&mut p1, &mut p2);
            }
            sb.push_str(&entry.rank.to_string());
            sb.push(':');
            sb.push_str(&p1.join(","));
            sb.push('|');
            sb.push_str(&p2.join(","));
            sb.push(';');
        }
        if min_form.as_ref().is_none_or(|m| sb < *m) {
            min_form = Some(sb);
        }
    }
    min_form.unwrap_or_default()
}

/// Returns one representative schedule per symmetry class (equivalence under any
/// role-preserving permutation of node names — see [`role_preserving_mappings`]),
/// preserving the order in which representatives are first encountered. `archives`
/// names the subset of `nodes` that are archive nodes; the rest are treated as
/// clients and are only interchangeable with one another.
pub fn all_rank_partitions_unique(
    nodes: &[String],
    archives: &HashSet<String>,
    stop_rank: u64,
) -> Vec<Vec<RankPartitionEntry>> {
    let mut sorted = nodes.to_vec();
    sorted.sort();

    let mut seen: HashSet<String> = HashSet::new();
    let mut out = Vec::new();
    for schedule in all_rank_partitions(&sorted, stop_rank) {
        let cf = canonical_form(&schedule, &sorted, archives);
        if seen.insert(cf) {
            out.push(schedule);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    /// Treats every node as an archive, reproducing the full (role-blind)
    /// permutation group for tests that don't exercise role separation.
    fn all_archives(nodes: &[String]) -> HashSet<String> {
        nodes.iter().cloned().collect()
    }

    fn normalize(schedule: &[RankPartitionEntry]) -> Vec<RankPartitionEntry> {
        schedule
            .iter()
            .map(|e| {
                let mut p1 = e.partition1.clone();
                let mut p2 = e.partition2.clone();
                p1.sort();
                p2.sort();
                RankPartitionEntry {
                    rank: e.rank,
                    partition1: p1,
                    partition2: p2,
                }
            })
            .collect()
    }

    #[test]
    fn all_bipartitions_empty() {
        assert!(all_bipartitions(&[]).is_empty());
    }

    #[test]
    fn all_bipartitions_single_node() {
        assert!(all_bipartitions(&s(&["A"])).is_empty());
    }

    #[test]
    fn all_bipartitions_two_nodes() {
        let bps = all_bipartitions(&s(&["A", "B"]));
        assert_eq!(bps.len(), 1);
        assert_eq!(bps[0], [s(&["A"]), s(&["B"])]);
    }

    #[test]
    fn all_bipartitions_three_nodes_unsorted() {
        let bps = all_bipartitions(&s(&["C", "A", "B"]));
        assert_eq!(bps.len(), 3);
        for bp in &bps {
            assert_eq!(bp[0][0], "A", "expected 'A' first in partition1 of {bp:?}");
        }
    }

    #[test]
    fn all_rank_partitions_empty_nodes() {
        let schedules = all_rank_partitions_unique(&[], &HashSet::new(), 5);
        assert_eq!(schedules.len(), 1);
        assert!(schedules[0].is_empty());
    }

    #[test]
    fn all_rank_partitions_single_node() {
        let nodes = s(&["A"]);
        let schedules = all_rank_partitions_unique(&nodes, &all_archives(&nodes), 3);
        assert_eq!(schedules.len(), 1);
        assert!(schedules[0].is_empty());
    }

    #[test]
    fn all_rank_partitions_two_nodes_stop_rank1() {
        let want: Vec<Vec<RankPartitionEntry>> = vec![
            vec![],
            vec![RankPartitionEntry {
                rank: 0,
                partition1: s(&["A"]),
                partition2: s(&["B"]),
            }],
            vec![RankPartitionEntry {
                rank: 1,
                partition1: s(&["A"]),
                partition2: s(&["B"]),
            }],
            vec![
                RankPartitionEntry {
                    rank: 0,
                    partition1: s(&["A"]),
                    partition2: s(&["B"]),
                },
                RankPartitionEntry {
                    rank: 1,
                    partition1: s(&["A"]),
                    partition2: s(&["B"]),
                },
            ],
        ];
        let nodes = s(&["A", "B"]);
        let got = all_rank_partitions_unique(&nodes, &all_archives(&nodes), 1);
        assert_eq!(got, want);
    }

    #[test]
    fn all_rank_partitions_two_nodes_stop_rank0() {
        let want: Vec<Vec<RankPartitionEntry>> = vec![
            vec![],
            vec![RankPartitionEntry {
                rank: 0,
                partition1: s(&["A"]),
                partition2: s(&["B"]),
            }],
        ];
        let nodes = s(&["A", "B"]);
        let got = all_rank_partitions_unique(&nodes, &all_archives(&nodes), 0);
        assert_eq!(got, want);
    }

    #[test]
    fn all_rank_partitions_three_nodes_stop_rank2_count() {
        let nodes = s(&["A", "B", "C"]);
        let mut count = 0;
        for p in all_rank_partitions_unique(&nodes, &all_archives(&nodes), 2) {
            for entry in &p {
                assert!(entry.rank <= 2, "got rank {} > 2", entry.rank);
                assert_eq!(
                    entry.partition1.len() + entry.partition2.len(),
                    3,
                    "partitions do not cover all nodes: {:?} + {:?}",
                    entry.partition1,
                    entry.partition2
                );
                let mut set: HashSet<&str> = HashSet::new();
                set.extend(entry.partition1.iter().map(String::as_str));
                set.extend(entry.partition2.iter().map(String::as_str));
                assert_eq!(set.len(), 3, "partitions do not cover all nodes");
            }
            count += 1;
        }
        assert_eq!(count, 15);
    }

    #[test]
    fn symmetric_dedup() {
        // s1: Rank0:[A,B]|[C], Rank1:[A]|[B,C]
        // s2: Rank0:[A,C]|[B], Rank1:[A]|[B,C]
        // are symmetric (swap B and C), so at most one should appear.
        let s1 = normalize(&[
            RankPartitionEntry {
                rank: 0,
                partition1: s(&["A", "B"]),
                partition2: s(&["C"]),
            },
            RankPartitionEntry {
                rank: 1,
                partition1: s(&["A"]),
                partition2: s(&["B", "C"]),
            },
        ]);
        let s2 = normalize(&[
            RankPartitionEntry {
                rank: 0,
                partition1: s(&["A", "C"]),
                partition2: s(&["B"]),
            },
            RankPartitionEntry {
                rank: 1,
                partition1: s(&["A"]),
                partition2: s(&["B", "C"]),
            },
        ]);

        let nodes = s(&["A", "B", "C"]);
        let mut found_s1 = false;
        let mut found_s2 = false;
        for schedule in all_rank_partitions_unique(&nodes, &all_archives(&nodes), 2) {
            let ns = normalize(&schedule);
            if ns == s1 {
                found_s1 = true;
            }
            if ns == s2 {
                found_s2 = true;
            }
        }
        assert!(!(found_s1 && found_s2), "both symmetric schedules appear");
        assert!(found_s1 || found_s2, "neither symmetric schedule appears");
    }

    #[test]
    fn symmetric_dedup_two_pair_swap() {
        // With 4 nodes, symmetric under the double transposition A↔C, B↔D.
        let s1 = normalize(&[
            RankPartitionEntry {
                rank: 0,
                partition1: s(&["A"]),
                partition2: s(&["B", "C", "D"]),
            },
            RankPartitionEntry {
                rank: 1,
                partition1: s(&["A", "B"]),
                partition2: s(&["C", "D"]),
            },
        ]);
        let s2 = normalize(&[
            RankPartitionEntry {
                rank: 0,
                partition1: s(&["A", "B", "D"]),
                partition2: s(&["C"]),
            },
            RankPartitionEntry {
                rank: 1,
                partition1: s(&["A", "B"]),
                partition2: s(&["C", "D"]),
            },
        ]);

        let nodes = s(&["A", "B", "C", "D"]);
        let mut found_s1 = false;
        let mut found_s2 = false;
        for schedule in all_rank_partitions_unique(&nodes, &all_archives(&nodes), 1) {
            let ns = normalize(&schedule);
            if ns == s1 {
                found_s1 = true;
            }
            if ns == s2 {
                found_s2 = true;
            }
        }
        assert!(!(found_s1 && found_s2), "both symmetric schedules appear");
        assert!(found_s1 || found_s2, "neither symmetric schedule appears");
    }

    /// Role-aware dedup must NOT collapse a client-isolating schedule with an
    /// archive-isolating one: archives and clients play different roles, so
    /// "isolate the client" and "isolate an archive" are distinct scenarios.
    /// A role-blind dedup (permuting the client with an archive) would keep only
    /// one of the two.
    #[test]
    fn role_aware_keeps_client_and_archive_partitions_distinct() {
        let nodes = s(&["archive-1", "archive-2", "client-1"]);
        let archives: HashSet<String> = s(&["archive-1", "archive-2"]).into_iter().collect();
        let schedules = all_rank_partitions_unique(&nodes, &archives, 0);

        // Two archive-vs-client splits (isolate archive-1 / isolate archive-2)
        // are equivalent under the archive↔archive swap, so at rank 0 there are
        // exactly: the empty schedule, "isolate one archive", "isolate client".
        assert_eq!(schedules.len(), 3, "got {schedules:?}");

        let isolates = |node: &str| {
            schedules.iter().any(|sch| {
                sch.iter().any(|e| {
                    e.partition1 == vec![node.to_string()] || e.partition2 == vec![node.to_string()]
                })
            })
        };
        assert!(isolates("client-1"), "no schedule isolates the client");
        assert!(
            isolates("archive-1") || isolates("archive-2"),
            "no schedule isolates an archive"
        );
    }

    #[test]
    fn parse_rank_partitions_basic() {
        let raw = r#"[{"rank":5,"partition1":["archive-1"],"partition2":["archive-3"]}]"#;
        let m = parse_rank_partitions(raw).unwrap();
        assert_eq!(m.len(), 1);
        let e = &m[&5];
        assert_eq!(e.partition1, s(&["archive-1"]));
        assert_eq!(e.partition2, s(&["archive-3"]));
    }

    #[test]
    fn parse_rank_partitions_missing_field() {
        let raw = r#"[{"rank":5,"partition1":["archive-1"]}]"#;
        let err = parse_rank_partitions(raw).unwrap_err().to_string();
        assert!(err.contains("partition2"), "unexpected error: {err}");
    }

    #[test]
    fn parse_rank_partitions_duplicate_rank() {
        let raw = r#"[
            {"rank":1,"partition1":["A"],"partition2":["B"]},
            {"rank":1,"partition1":["A"],"partition2":["B"]}
        ]"#;
        let err = parse_rank_partitions(raw).unwrap_err().to_string();
        assert!(err.contains("duplicate rank"), "unexpected error: {err}");
    }
}
