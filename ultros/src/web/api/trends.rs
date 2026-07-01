use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use tracing::instrument;
use ultros_api_types::trends::TrendsData;
use ultros_db::world_data::world_cache::{AnySelector, WorldCache};

use crate::{analyzer_service::AnalyzerService, web::error::WebError};

#[derive(Debug, Deserialize, Default)]
pub struct TrendsQuery {
    /// One of 7, 30, or 90 — selects the v2 CH-backed window aggregate.
    /// When omitted the endpoint returns the legacy pre-bucketed payload
    /// (`high_velocity` / `rising_price` / `falling_price`) for backward
    /// compatibility with any existing API consumer.
    pub window: Option<u16>,
    /// `1` / `true` bypasses the cross-cutting `ResaleQualityFilter` so
    /// suspicious rows surface with a chip. Default false.
    pub show_suspicious: Option<String>,
}

#[instrument(skip(analyzer, world_cache))]
pub async fn get_trends(
    State(analyzer): State<AnalyzerService>,
    State(world_cache): State<Arc<WorldCache>>,
    Path(world_name): Path<String>,
    Query(query): Query<TrendsQuery>,
) -> Result<Json<TrendsData>, WebError> {
    let selector = world_cache
        .lookup_value_by_name(&world_name)
        .map_err(|_| WebError::NotFound)?;
    let selector = AnySelector::from(&selector);

    // Currently only supporting trends for specific Worlds, as AnalyzerService::get_trends takes a world_id
    // If we want DC trends, we'd need to aggregate or the AnalyzerService needs to support it.
    // For now, if it's a datacenter, we error or pick a default?
    // Let's stick to World for now, or map DC to its worlds and aggregate?
    // Aggregating is expensive. Let's just enforce World for V1.

    let world_id = match selector {
        AnySelector::World(id) => id,
        // DC/Region aggregation is expensive to do on-the-fly. Return an empty
        // response rather than a 400 — callers should resolve to a specific
        // world before calling this endpoint.
        _ => {
            return Ok(Json(TrendsData {
                items: vec![],
                high_velocity: vec![],
                rising_price: vec![],
                falling_price: vec![],
            }));
        }
    };

    // V2 path: ?window= supplied → return a flat sorted list under
    // `items`. Clamp the window to the values the rollup actually
    // produces (7/30/90); anything else falls back to 30.
    if let Some(raw_window) = query.window {
        let window_days = match raw_window {
            7 | 30 | 90 => raw_window,
            _ => 30,
        };
        let include_suspicious =
            matches!(query.show_suspicious.as_deref(), Some("true") | Some("1"));
        let items = analyzer
            .get_trends_v2(world_id, window_days, include_suspicious)
            .await
            .unwrap_or_default();
        return Ok(Json(TrendsData {
            items,
            high_velocity: vec![],
            rising_price: vec![],
            falling_price: vec![],
        }));
    }

    // Legacy v1 path — pre-bucketed lists, kept for any older client.
    let trends = analyzer.get_trends(world_id).await.unwrap_or(TrendsData {
        items: vec![],
        high_velocity: vec![],
        rising_price: vec![],
        falling_price: vec![],
    });

    Ok(Json(trends))
}
