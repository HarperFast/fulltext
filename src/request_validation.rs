use std::collections::HashSet;

use crate::error::{FulltextError, Result};
use crate::protocol::{
	validate_record_id, validate_search_structure, validate_search_window, SearchExpression, SearchMode, SearchRequest,
	TraceRecord, MAX_AUTOCOMPLETE_RESULTS, MAX_CANDIDATE_BYTES, MAX_CANDIDATE_IDS, MAX_CONFIGURED_QUERY_CLAUSES,
	MAX_FIELDS, MAX_QUERY_TEXT_BYTES, MAX_TRACE_RECORDS, MAX_TRACE_SOURCE_BYTES,
};

pub(crate) fn validate_candidates(ids: Option<&[String]>) -> Result<()> {
	let Some(ids) = ids else { return Ok(()) };
	if ids.len() > MAX_CANDIDATE_IDS {
		return Err(FulltextError::invalid("candidate ID count exceeds 1024"));
	}
	let mut bytes = 0usize;
	for id in ids {
		validate_record_id(id)?;
		bytes = bytes.saturating_add(id.len());
		if bytes > MAX_CANDIDATE_BYTES {
			return Err(FulltextError::invalid("candidate IDs exceed 1048576 UTF-8 bytes"));
		}
	}
	Ok(())
}

pub(crate) fn validate_search_admission(request: &SearchRequest) -> Result<()> {
	let has_catalog = request.sort.is_some() || !request.aggregations.is_empty();
	validate_search_window(request.offset, request.limit, request.traversal, has_catalog)?;
	validate_candidates(request.candidate_ids.as_deref())?;
	// Bound the options/prefix walk before inspecting a caller-owned typed AST.
	validate_search_structure(&request.expression, MAX_CONFIGURED_QUERY_CLAUSES, MAX_FIELDS)?;
	let contains_prefix = validate_expression_options(&request.expression)?;
	if contains_prefix && !request.traversal && (request.offset != 0 || request.limit > MAX_AUTOCOMPLETE_RESULTS) {
		return Err(FulltextError::invalid(if has_catalog {
			"prefix search with sort or aggregations requires offset zero and a limit no greater than 100; metadata continuation is not supported"
		} else {
			"prefix search requires offset zero and a limit no greater than 100; use searchPage for subsequent matches"
		}));
	}
	if let Some(after) = &request.after {
		validate_record_id(&after.id)?;
		if !after.score.is_finite()
			|| after.snapshot.len() != 64
			|| !after
				.snapshot
				.bytes()
				.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
		{
			return Err(FulltextError::invalid("invalid search continuation boundary"));
		}
	}
	Ok(())
}

fn validate_expression_options(expression: &SearchExpression) -> Result<bool> {
	match expression {
		SearchExpression::Clause(clause) => {
			clause.options.validate(clause.mode)?;
			Ok(matches!(clause.mode, SearchMode::Prefix | SearchMode::FuzzyPrefix))
		}
		SearchExpression::And(children) | SearchExpression::Or(children) => {
			let mut contains_prefix = false;
			for child in children {
				contains_prefix |= validate_expression_options(child)?;
			}
			Ok(contains_prefix)
		}
		SearchExpression::Boost { clause, .. } => validate_expression_options(clause),
		SearchExpression::Optional { required, optional } => {
			let mut contains_prefix = validate_expression_options(required)?;
			for clause in optional {
				contains_prefix |= validate_expression_options(clause)?;
			}
			Ok(contains_prefix)
		}
		SearchExpression::Not(child) => validate_expression_options(child),
	}
}

pub(crate) fn validate_trace_admission(request: &SearchRequest, records: &[TraceRecord]) -> Result<()> {
	validate_candidates(request.candidate_ids.as_deref())?;
	let SearchExpression::Clause(clause) = &request.expression else {
		return Err(FulltextError::invalid(
			"traceMatches does not support boolean expressions",
		));
	};
	clause.options.validate(clause.mode)?;
	if clause.text.len() > MAX_QUERY_TEXT_BYTES {
		return Err(FulltextError::invalid("search text exceeds 65536 UTF-8 bytes"));
	}
	if clause.fields.len() > MAX_FIELDS {
		return Err(FulltextError::invalid("trace field count exceeds 1024 fields"));
	}
	if records.len() > MAX_TRACE_RECORDS {
		return Err(FulltextError::invalid("trace record count exceeds 100"));
	}
	let mut seen_ids = HashSet::with_capacity(records.len());
	let mut source_bytes = 0usize;
	for record in records {
		validate_record_id(&record.id)?;
		if !seen_ids.insert(record.id.as_str()) {
			return Err(FulltextError::invalid(format!("duplicate trace record {}", record.id)));
		}
		if record.fields.len() > MAX_FIELDS {
			return Err(FulltextError::invalid("trace record field count exceeds 1024 fields"));
		}
		let mut seen_fields = HashSet::with_capacity(record.fields.len());
		for (name, values) in &record.fields {
			if !seen_fields.insert(name.as_str()) {
				return Err(FulltextError::invalid(format!("duplicate trace field {name}")));
			}
			if values.len() > u16::MAX as usize {
				return Err(FulltextError::invalid("trace value count exceeds 65535"));
			}
			for value in values {
				source_bytes = source_bytes.saturating_add(value.len());
				if source_bytes > MAX_TRACE_SOURCE_BYTES {
					return Err(FulltextError::invalid("trace source text exceeds 1048576 UTF-8 bytes"));
				}
			}
		}
	}
	Ok(())
}

pub(crate) fn validate_filter_number(value: f64) -> Result<()> {
	if !value.is_finite() {
		return Err(FulltextError::invalid("filter numbers must be finite"));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::protocol::{QueryOptions, SearchBoundary, SearchClause};

	fn request(mode: SearchMode) -> SearchRequest {
		SearchRequest {
			sort: None,
			aggregations: Vec::new(),
			expression: SearchExpression::Clause(SearchClause {
				text: "alpha".into(),
				mode,
				fields: Vec::new(),
				options: QueryOptions::default(),
			}),
			candidate_ids: None,
			filter: None,
			offset: 0,
			limit: 10,
			exact_total: false,
			budget_milliseconds: 0,
			traversal: false,
			after: None,
		}
	}

	fn invalid(result: Result<()>) {
		assert_eq!(result.unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn direct_scoring_requests_validate_boosts_and_optional_prefixes() {
		let base = request(SearchMode::Any);
		for boost in [-0.0, -1.0, 101.0, f32::NAN, f32::INFINITY] {
			let mut query = base.clone();
			query.expression = SearchExpression::Boost {
				boost,
				clause: Box::new(base.expression.clone()),
			};
			invalid(validate_search_admission(&query));
		}
		for boost in [0.0, 0.5, 1.0, 100.0] {
			let mut query = base.clone();
			query.expression = SearchExpression::Boost {
				boost,
				clause: Box::new(base.expression.clone()),
			};
			validate_search_admission(&query).unwrap();
		}
		let mut query = base.clone();
		query.expression = SearchExpression::Optional {
			required: Box::new(base.expression.clone()),
			optional: Vec::new(),
		};
		invalid(validate_search_admission(&query));
		query.expression = SearchExpression::Optional {
			required: Box::new(base.expression),
			optional: vec![SearchExpression::Boost {
				boost: 2.0,
				clause: Box::new(request(SearchMode::Prefix).expression),
			}],
		};
		query.offset = 1;
		invalid(validate_search_admission(&query));
		query.offset = 0;
		query.limit = 101;
		invalid(validate_search_admission(&query));
		query.limit = 10;
		validate_search_admission(&query).unwrap();
	}

	#[test]
	fn candidates_count_original_entries_and_bytes() {
		validate_candidates(None).unwrap();
		validate_candidates(Some(&[])).unwrap();
		validate_candidates(Some(&vec!["x".into(); MAX_CANDIDATE_IDS])).unwrap();
		invalid(validate_candidates(Some(&vec!["x".into(); MAX_CANDIDATE_IDS + 1])));
		let id = "x".repeat(crate::protocol::MAX_RECORD_ID_BYTES);
		validate_candidates(Some(&vec![id.clone(); 256])).unwrap();
		invalid(validate_candidates(Some(&vec![id; 257])));
		invalid(validate_candidates(Some(&[String::new()])));
	}

	#[test]
	fn autocomplete_window_applies_to_nested_prefixes_but_not_traversal() {
		for mode in [SearchMode::Prefix, SearchMode::FuzzyPrefix] {
			let mut request = request(mode);
			request.expression = SearchExpression::Not(Box::new(request.expression));
			request.limit = MAX_AUTOCOMPLETE_RESULTS;
			validate_search_admission(&request).unwrap();
			request.limit += 1;
			invalid(validate_search_admission(&request));
			request.traversal = true;
			validate_search_admission(&request).unwrap();
			request.traversal = false;
			request.limit = 1;
			request.offset = 1;
			invalid(validate_search_admission(&request));
		}
	}

	#[test]
	fn search_admission_rejects_malformed_structure_and_boundary_before_snapshot_work() {
		let mut request = request(SearchMode::Any);
		request.traversal = true;
		request.after = Some(SearchBoundary {
			snapshot: "0".repeat(64),
			score: 0.0,
			id: "id".into(),
		});
		validate_search_admission(&request).unwrap();
		request.after.as_mut().unwrap().score = f32::NAN;
		invalid(validate_search_admission(&request));
		request.after.as_mut().unwrap().score = 0.0;
		request.after.as_mut().unwrap().id.clear();
		invalid(validate_search_admission(&request));
		request.after.as_mut().unwrap().id = "id".into();
		request.expression = SearchExpression::And(Vec::new());
		invalid(validate_search_admission(&request));
	}

	#[test]
	fn admission_uses_supported_structure_ceiling_without_enforcing_default_compiled_cost() {
		let mut request = request(SearchMode::Any);
		request.expression = SearchExpression::And(vec![request.expression.clone(); 257]);
		validate_search_admission(&request).unwrap();
		request.expression = SearchExpression::And(vec![request.expression.clone(); 17]);
		invalid(validate_search_admission(&request));
	}

	#[test]
	fn options_are_validated_in_every_branch_and_trace() {
		let mut request = request(SearchMode::Any);
		let mut invalid_clause = request.expression.clone();
		if let SearchExpression::Clause(clause) = &mut invalid_clause {
			clause.options.fuzzy_distance = 0;
		}
		request.expression = SearchExpression::Or(vec![request.expression.clone(), invalid_clause.clone()]);
		invalid(validate_search_admission(&request));
		request.expression = invalid_clause;
		invalid(validate_trace_admission(&request, &[]));
		if let SearchExpression::Clause(clause) = &mut request.expression {
			clause.mode = SearchMode::Fuzzy;
		}
		validate_search_admission(&request).unwrap();
		validate_trace_admission(&request, &[]).unwrap();
	}

	#[test]
	fn trace_limits_apply_even_with_empty_candidates() {
		let mut request = request(SearchMode::Any);
		request.limit = 0;
		request.offset = usize::MAX;
		request.candidate_ids = Some(Vec::new());
		validate_trace_admission(&request, &[]).unwrap();
		let record = TraceRecord {
			id: "id".into(),
			fields: Vec::new(),
		};
		invalid(validate_trace_admission(
			&request,
			&vec![record.clone(); MAX_TRACE_RECORDS + 1],
		));
		invalid(validate_trace_admission(&request, &[record.clone(), record]));
		let mut record = TraceRecord {
			id: "id".into(),
			fields: vec![("body".into(), vec!["x".repeat(MAX_TRACE_SOURCE_BYTES)])],
		};
		validate_trace_admission(&request, std::slice::from_ref(&record)).unwrap();
		record.fields[0].1.push("x".into());
		invalid(validate_trace_admission(&request, &[record]));
		if let SearchExpression::Clause(clause) = &mut request.expression {
			clause.text = " ".repeat(MAX_QUERY_TEXT_BYTES + 1);
		}
		invalid(validate_trace_admission(&request, &[]));
	}

	#[test]
	fn trace_rejects_duplicate_fields_and_unrepresentable_value_counts() {
		let request = request(SearchMode::Any);
		let mut record = TraceRecord {
			id: "id".into(),
			fields: vec![("body".into(), Vec::new()); 2],
		};
		invalid(validate_trace_admission(&request, std::slice::from_ref(&record)));
		record.fields.truncate(1);
		record.fields[0].1 = vec![String::new(); u16::MAX as usize + 1];
		invalid(validate_trace_admission(&request, &[record]));
	}

	#[test]
	fn numeric_filters_require_finite_values_and_preserve_both_zeros() {
		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			invalid(validate_filter_number(value));
		}
		for value in [0.0, -0.0, f64::MAX, f64::MIN] {
			validate_filter_number(value).unwrap();
		}
	}
}
