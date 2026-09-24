//! Deterministic line differences with explicit computational and output bounds.
use super::{GatewayError, check_deadline};
use agent_knowledge_protocol::{BodyDiff, BodyHunks, DiffTruncation};
use std::time::Instant;

const MAXIMUM_LINES: usize = 20_000;
const MAXIMUM_TRACE_CELLS: usize = 1_000_000;
const MAXIMUM_WORK: usize = 1_000_000;

#[derive(Clone, Copy)]
struct Range {
    old_start: usize,
    old_end: usize,
    new_start: usize,
    new_end: usize,
}

pub(super) fn hunks(
    before: &str,
    after: &str,
    context: usize,
    maximum_hunks: usize,
    maximum_bytes: usize,
    deadline: Instant,
) -> Result<BodyHunks, GatewayError> {
    check_deadline(deadline)?;
    let mut result = BodyHunks {
        changed: before != after,
        hunks: Vec::new(),
        truncated: false,
        truncation_reason: None,
    };
    if !result.changed {
        return Ok(result);
    }
    let old = before
        .split_inclusive('\n')
        .take(MAXIMUM_LINES + 1)
        .collect::<Vec<_>>();
    let new = after
        .split_inclusive('\n')
        .take(MAXIMUM_LINES + 1)
        .collect::<Vec<_>>();
    if old.len() > MAXIMUM_LINES || new.len() > MAXIMUM_LINES {
        return Ok(truncate(result, DiffTruncation::InputLineLimit));
    }
    let prefix = old.iter().zip(&new).take_while(|(a, b)| a == b).count();
    let suffix = old[prefix..]
        .iter()
        .rev()
        .zip(new[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let old_end = old.len() - suffix;
    let new_end = new.len() - suffix;
    let m = old_end - prefix;
    let n = new_end - prefix;
    let ranges = if m == 0 || n == 0 {
        vec![Range {
            old_start: prefix,
            old_end,
            new_start: prefix,
            new_end,
        }]
    } else {
        let Some(ranges) = changes(
            &old[prefix..old_end],
            &new[prefix..new_end],
            prefix,
            deadline,
        )?
        else {
            return Ok(truncate(result, DiffTruncation::ComputationLimit));
        };
        ranges
    };
    let mut merged: Vec<Range> = Vec::new();
    for range in ranges {
        check_deadline(deadline)?;
        let expanded = Range {
            old_start: range.old_start.saturating_sub(context),
            old_end: (range.old_end + context).min(old.len()),
            new_start: range.new_start.saturating_sub(context),
            new_end: (range.new_end + context).min(new.len()),
        };
        if let Some(last) = merged.last_mut()
            && (expanded.old_start <= last.old_end || expanded.new_start <= last.new_end)
        {
            last.old_end = expanded.old_end;
            last.new_end = expanded.new_end;
        } else {
            merged.push(expanded);
        }
    }
    for range in merged {
        check_deadline(deadline)?;
        if result.hunks.len() == maximum_hunks {
            return Ok(truncate(result, DiffTruncation::HunkLimit));
        }
        let removed = &old[range.old_start..range.old_end];
        let added = &new[range.new_start..range.new_end];
        // Reject large ranges before copying their text or escaping it as JSON.
        let bytes = removed
            .iter()
            .chain(added)
            .map(|line| line.len())
            .sum::<usize>();
        if bytes > maximum_bytes {
            return Ok(truncate(result, DiffTruncation::ByteLimit));
        }
        result.hunks.push(BodyDiff {
            from_line: range.old_start + 1,
            to_line: range.new_start + 1,
            removed: removed.concat(),
            added: added.concat(),
        });
        // Reserve space for the longest explicit truncation reason. The byte
        // budget includes the entire BodyHunks JSON object, including escapes.
        let bytes = serde_json::to_vec(&result)
            .map_err(|_| super::invalid())?
            .len();
        if bytes.saturating_add(32) > maximum_bytes {
            result.hunks.pop();
            return Ok(truncate(result, DiffTruncation::ByteLimit));
        }
    }
    check_deadline(deadline)?;
    Ok(result)
}

fn truncate(mut result: BodyHunks, reason: DiffTruncation) -> BodyHunks {
    result.truncated = true;
    result.truncation_reason = Some(reason);
    result
}

fn changes(
    old: &[&str],
    new: &[&str],
    offset: usize,
    deadline: Instant,
) -> Result<Option<Vec<Range>>, GatewayError> {
    // A bounded Myers frontier follows equal runs without visiting a quadratic
    // line-pair matrix. Trace storage and work are independently bounded.
    let maximum_distance = (old.len() + new.len()).min(1024);
    let center = maximum_distance + 1;
    let width = 2 * maximum_distance + 3;
    let mut frontier = vec![0_isize; width];
    let mut trace = Vec::new();
    let mut work = 0;
    for distance in 0..=maximum_distance {
        check_deadline(deadline)?;
        if (trace.len() + 1) * width > MAXIMUM_TRACE_CELLS {
            return Ok(None);
        }
        trace.push(frontier.clone());
        let depth = distance as isize;
        for diagonal in (-depth..=depth).step_by(2) {
            work += 1;
            if work > MAXIMUM_WORK {
                return Ok(None);
            }
            let at = (center as isize + diagonal) as usize;
            let mut x = if diagonal == -depth
                || (diagonal != depth && frontier[at - 1] < frontier[at + 1])
            {
                frontier[at + 1]
            } else {
                frontier[at - 1] + 1
            };
            let mut y = x - diagonal;
            while x < old.len() as isize
                && y < new.len() as isize
                && old[x as usize] == new[y as usize]
            {
                x += 1;
                y += 1;
                work += 1;
                if work > MAXIMUM_WORK {
                    return Ok(None);
                }
                if work % 256 == 0 {
                    check_deadline(deadline)?;
                }
            }
            frontier[at] = x;
            if x == old.len() as isize && y == new.len() as isize {
                return reconstruct(old.len(), new.len(), offset, center, &trace, deadline)
                    .map(Some);
            }
        }
    }
    Ok(None)
}

fn reconstruct(
    old_len: usize,
    new_len: usize,
    offset: usize,
    center: usize,
    trace: &[Vec<isize>],
    deadline: Instant,
) -> Result<Vec<Range>, GatewayError> {
    let (mut x, mut y) = (old_len as isize, new_len as isize);
    let mut matches = Vec::new();
    for distance in (0..trace.len()).rev() {
        check_deadline(deadline)?;
        let depth = distance as isize;
        let diagonal = x - y;
        let at = (center as isize + diagonal) as usize;
        let previous = if diagonal == -depth
            || (diagonal != depth && trace[distance][at - 1] < trace[distance][at + 1])
        {
            diagonal + 1
        } else {
            diagonal - 1
        };
        let previous_x = trace[distance][(center as isize + previous) as usize];
        let previous_y = previous_x - previous;
        while x > previous_x && y > previous_y {
            x -= 1;
            y -= 1;
            matches.push((x as usize, y as usize));
        }
        x = previous_x;
        y = previous_y;
    }
    let (mut a, mut b) = (0, 0);
    let mut ranges = Vec::new();
    for (x, y) in matches
        .into_iter()
        .rev()
        .chain(std::iter::once((old_len, new_len)))
    {
        check_deadline(deadline)?;
        if x > a || y > b {
            ranges.push(Range {
                old_start: offset + a,
                old_end: offset + x,
                new_start: offset + b,
                new_end: offset + y,
            });
        }
        a = x + 1;
        b = y + 1;
    }
    Ok(ranges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn diff(old: &str, new: &str, context: usize, count: usize, bytes: usize) -> BodyHunks {
        hunks(
            old,
            new,
            context,
            count,
            bytes,
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap_or_else(|e| panic!("diff: {e}"))
    }

    fn apply(old: &str, body: &BodyHunks) -> String {
        let lines = old.split_inclusive('\n').collect::<Vec<_>>();
        let mut output = String::new();
        let mut position = 0;
        for hunk in &body.hunks {
            let start = hunk.from_line - 1;
            output.push_str(&lines[position..start].concat());
            let count = hunk.removed.split_inclusive('\n').count();
            assert_eq!(lines[start..start + count].concat(), hunk.removed);
            assert_eq!(output.split_inclusive('\n').count() + 1, hunk.to_line);
            output.push_str(&hunk.added);
            position = start + count;
        }
        output.push_str(&lines[position..].concat());
        output
    }

    #[test]
    fn distant_changes_omit_large_unchanged_middle_and_preserve_context() {
        let middle = (0..5000)
            .map(|i| format!("unchanged paragraph {i}\n"))
            .collect::<String>();
        let old = format!("intro\nold paragraph\n{middle}source A\nend\n");
        let new = format!("intro\nrevised paragraph\n{middle}source B\nend\n");
        let result = diff(&old, &new, 2, 20, 64000);
        assert_eq!(result.hunks.len(), 2);
        assert!(!result.truncated);
        assert!(
            result
                .hunks
                .iter()
                .all(|h| !h.removed.contains("unchanged paragraph 2500"))
        );
        assert_eq!(apply(&old, &result), new);
    }

    #[test]
    fn ranges_reconstruct_unicode_insertions_deletions_and_newline_only_changes() {
        for (old, new) in [
            ("", "new"),
            ("gone\n", ""),
            ("same\n", "same\n"),
            ("a\nlast\n", "a\n追加\nlast\n"),
            ("削除\nx\nlast", "x\nlast"),
            ("a\n", "a"),
            ("a\r\nb\r\n", "a\nb\n"),
            ("a\nb\na\nb\n", "b\na\nb\na\n"),
        ] {
            for context in [0, 1, 3] {
                let result = diff(old, new, context, 20, 64000);
                assert_eq!(result.changed, old != new);
                assert!(!result.truncated);
                assert_eq!(apply(old, &result), new);
            }
        }
    }

    #[test]
    fn hunk_byte_compute_and_line_limits_are_explicit() {
        let old = "a\none\nb\nc\nd\ne\ntwo\nz\n";
        let new = "a\nONE\nb\nc\nd\ne\nTWO\nz\n";
        let limited = diff(old, new, 0, 1, 64000);
        assert_eq!(limited.hunks.len(), 1);
        assert_eq!(limited.truncation_reason, Some(DiffTruncation::HunkLimit));
        let large_later = format!("a\nONE\nb\nc\nd\ne\n{}\nz\n", "界".repeat(200));
        let partial = diff(old, &large_later, 0, 20, 256);
        assert_eq!(partial.hunks.len(), 1);
        assert_eq!(partial.truncation_reason, Some(DiffTruncation::ByteLimit));
        assert!(
            serde_json::to_vec(&partial)
                .unwrap_or_else(|e| panic!("JSON: {e}"))
                .len()
                <= 256
        );
        for large in ["界".repeat(200), "\u{1}".repeat(200)] {
            let result = diff("", &large, 0, 20, 256);
            assert!(result.changed && result.truncated);
            assert_eq!(result.truncation_reason, Some(DiffTruncation::ByteLimit));
            assert!(
                serde_json::to_vec(&result)
                    .unwrap_or_else(|e| panic!("JSON: {e}"))
                    .len()
                    <= 256
            );
        }
        let result = diff(&"old\n".repeat(1100), &"new\n".repeat(1100), 0, 20, 64000);
        assert_eq!(
            result.truncation_reason,
            Some(DiffTruncation::ComputationLimit)
        );
        let result = diff("", &"line\n".repeat(MAXIMUM_LINES + 1), 0, 20, 64000);
        assert_eq!(
            result.truncation_reason,
            Some(DiffTruncation::InputLineLimit)
        );
        assert!(matches!(
            hunks("a", "b", 0, 1, 256, Instant::now()),
            Err(GatewayError::OperationDeadlineExceeded)
        ));
    }
    #[test]
    fn bounded_search_reconstructs_all_short_repeated_line_sequences() {
        let mut sequences = vec![String::new()];
        for length in 1..=4 {
            for bits in 0..(1 << length) {
                sequences.push(
                    (0..length)
                        .map(|bit| if bits & (1 << bit) == 0 { "a\n" } else { "b\n" })
                        .collect::<String>(),
                );
            }
        }
        for old in &sequences {
            for new in &sequences {
                for context in [0, 1, 3] {
                    let result = diff(old, new, context, 20, 64000);
                    assert!(!result.truncated);
                    assert_eq!(apply(old, &result), *new);
                }
            }
        }
    }
}
