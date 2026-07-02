use crate::api::*;
use crate::components::icon::Icon;
use crate::components::item_icon::{IconSize, ItemIcon};
use crate::components::world_name::WorldName;
use crate::components::world_picker::WorldPicker;
use crate::global_state::LocalWorldData;
use crate::global_state::xiv_data::tracked_data;
use icondata as i;
use leptos::either::Either;
use leptos::prelude::*;
use leptos::reactive::wrappers::write::IntoSignalSetter;
use leptos::task::spawn_local;
use serde_json::json;
use thousands::Separable;
use ultros_api_types::world_helper::AnySelector;
#[cfg(feature = "hydrate")]
use wasm_bindgen::JsCast;

#[derive(Clone, Copy, Debug, PartialEq)]
enum MarketTab {
    Dashboard,
    Arbitrage,
    Crafting,
    Gathering,
    Settings,
}

#[component]
pub fn MarketDashboard() -> impl IntoView {
    let (active_tab, set_active_tab) = signal(MarketTab::Dashboard);

    // Profiles state
    let (profiles, set_profiles) = signal(Vec::<PlayerProfile>::new());
    let (active_profile, set_active_profile) = signal(None::<PlayerProfile>);
    let (is_authenticated, set_is_authenticated) = signal(true);
    let (setup_status, set_setup_status) = signal(None::<ProfileSetupStatus>);

    // Selected profile ID helper
    let active_profile_id = move || active_profile().map(|p| p.id);

    // Fetch user profiles
    let load_profiles = move || {
        spawn_local(async move {
            match get_profiles().await {
                Ok(list) => {
                    let active_id = active_profile().map(|p| p.id);
                    set_profiles(list.clone());
                    set_is_authenticated(true);
                    if list.is_empty() {
                        set_active_profile(None);
                    } else if let Some(id) = active_id {
                        let next_profile = list
                            .iter()
                            .find(|p| p.id == id)
                            .cloned()
                            .unwrap_or_else(|| list[0].clone());
                        set_active_profile(Some(next_profile));
                    } else {
                        set_active_profile(Some(list[0].clone()));
                    }
                }
                Err(e) => {
                    log::error!("Error loading profiles: {e:?}");
                    if let crate::error::AppError::ApiError(
                        ultros_api_types::result::ApiError::NotAuthenticated,
                    ) = e
                    {
                        set_is_authenticated(false);
                    }
                }
            }
        });
    };

    // Initial load
    Effect::new(move |_| {
        load_profiles();
    });

    Effect::new(move |_| {
        if let Some(profile) = active_profile() {
            spawn_local(async move {
                match get_profile_setup_status(profile.id).await {
                    Ok(status) => set_setup_status(Some(status)),
                    Err(e) => {
                        log::error!("Error loading profile setup status: {e:?}");
                        set_setup_status(None);
                    }
                }
            });
        } else {
            set_setup_status(None);
        }
    });

    // Eorzea Clock Signal
    let (eorzea_time_str, set_eorzea_time_str) = signal("00:00".to_string());

    // Tick Eorzea clock
    #[cfg(feature = "hydrate")]
    {
        let interval =
            send_wrapper::SendWrapper::new(gloo_timers::callback::Interval::new(1000, move || {
                let now = js_sys::Date::now() / 1000.0;
                let eorzea_seconds = now * (1440.0 / 70.0);
                let total_minutes = (eorzea_seconds / 60.0) as u32;
                let hour = (total_minutes / 60) % 24;
                let minute = total_minutes % 60;
                set_eorzea_time_str(format!("{:02}:{:02} ET", hour, minute));
            }));
        on_cleanup(move || {
            drop(interval);
        });
    }

    // Health state from SSE
    let (health_status, set_health_status) = signal("Connecting...".to_string());
    let (health_color, set_health_color) = signal("text-amber-400 bg-amber-400/10".to_string());

    // Subscribe to SSE
    #[cfg(feature = "hydrate")]
    {
        let event_source = web_sys::EventSource::new("/api/v1/events");
        if let Ok(source) = event_source {
            let on_message =
                wasm_bindgen::prelude::Closure::<dyn FnMut(web_sys::MessageEvent)>::new(
                    move |event: web_sys::MessageEvent| {
                        if let Some(data) = event.data().as_string()
                            && let Ok(event_type) = serde_json::from_str::<serde_json::Value>(&data)
                        {
                            if event_type.as_str() == Some("Healthy") {
                                set_health_status("Healthy".to_string());
                                set_health_color(
                                    "text-emerald-400 bg-emerald-400/10 border-emerald-400/20"
                                        .to_string(),
                                );
                            } else if let Some(lag) = event_type.get("Lagging") {
                                if let Some(sec) = lag.get("lag_seconds").and_then(|v| v.as_i64()) {
                                    set_health_status(format!("Lagging ({}m)", sec / 60));
                                } else {
                                    set_health_status("Lagging".to_string());
                                }
                                set_health_color(
                                    "text-amber-400 bg-amber-400/10 border-amber-400/20"
                                        .to_string(),
                                );
                            } else if event_type.as_str() == Some("Disconnected") {
                                set_health_status("Disconnected".to_string());
                                set_health_color(
                                    "text-rose-400 bg-rose-400/10 border-rose-400/20 animate-pulse"
                                        .to_string(),
                                );
                            }
                        }
                    },
                );
            source.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

            let source_clone = send_wrapper::SendWrapper::new(source.clone());
            let on_message_wrapper = send_wrapper::SendWrapper::new(on_message);
            on_cleanup(move || {
                source_clone.close();
                drop(on_message_wrapper);
            });
        }
    }

    view! {
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8 text-gray-200">
            // Header Section
            <div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl">
                <div>
                    <h1 class="text-3xl font-extrabold tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-violet-400 to-fuchsia-400">
                        "Market Board Analytics & Arbitrage"
                    </h1>
                    <p class="text-sm text-gray-400 mt-1">
                        "High-frequency cross-world market arbitrage, recursive crafting optimization, & timed node ranker."
                    </p>
                </div>

                <div class="flex flex-wrap items-center gap-3">
                    // Eorzea Clock Badge
                    <div class="px-4 py-2 rounded-xl bg-violet-950/20 border border-violet-500/20 text-violet-300 font-mono text-sm font-semibold flex items-center gap-2">
                        <Icon icon=i::BiTimeFiveRegular />
                        <span>{move || eorzea_time_str()}</span>
                    </div>

                    // Health State pill
                    <div class=move || format!("px-4 py-2 rounded-xl border font-semibold text-sm flex items-center gap-2 {}", health_color())>
                        <span class="relative flex h-2 w-2">
                            <span class="animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 bg-current"></span>
                            <span class="relative inline-flex rounded-full h-2 w-2 bg-current"></span>
                        </span>
                        <span>"Feed: " {move || health_status()}</span>
                    </div>

                    // Profile Selector
                    <div class="relative">
                        <select
                            class="px-4 py-2 rounded-xl bg-zinc-900 border border-white/10 text-gray-200 focus:outline-none focus:border-violet-500/50 cursor-pointer"
                            on:change=move |ev| {
                                let val = event_target_value(&ev).parse::<i32>().unwrap_or(0);
                                if let Some(p) = profiles().iter().find(|x| x.id == val) {
                                    set_active_profile(Some(p.clone()));
                                }
                            }
                        >
                            {move || profiles().into_iter().map(|p| {
                                let is_selected = active_profile().map(|x| x.id == p.id).unwrap_or(false);
                                view! {
                                    <option value=p.id selected=is_selected>{p.display_name.clone()}</option>
                                }
                            }).collect::<Vec<_>>()}
                        </select>
                    </div>
                </div>
            </div>

            // Navigation Tabs
            <div class="flex border-b border-white/10 gap-2">
                <button
                    class=move || format!("px-5 py-3 font-semibold transition-all duration-300 border-b-2 -mb-[2px] {}",
                        if active_tab() == MarketTab::Dashboard { "border-violet-500 text-violet-400 bg-violet-500/5" } else { "border-transparent text-gray-400 hover:text-gray-200" })
                    on:click=move |_| set_active_tab(MarketTab::Dashboard)
                >
                    "Dashboard"
                </button>
                <button
                    class=move || format!("px-5 py-3 font-semibold transition-all duration-300 border-b-2 -mb-[2px] {}",
                        if active_tab() == MarketTab::Arbitrage { "border-violet-500 text-violet-400 bg-violet-500/5" } else { "border-transparent text-gray-400 hover:text-gray-200" })
                    on:click=move |_| set_active_tab(MarketTab::Arbitrage)
                >
                    "Arbitrage Flips"
                </button>
                <button
                    class=move || format!("px-5 py-3 font-semibold transition-all duration-300 border-b-2 -mb-[2px] {}",
                        if active_tab() == MarketTab::Crafting { "border-violet-500 text-violet-400 bg-violet-500/5" } else { "border-transparent text-gray-400 hover:text-gray-200" })
                    on:click=move |_| set_active_tab(MarketTab::Crafting)
                >
                    "Recursive Crafting"
                </button>
                <button
                    class=move || format!("px-5 py-3 font-semibold transition-all duration-300 border-b-2 -mb-[2px] {}",
                        if active_tab() == MarketTab::Gathering { "border-violet-500 text-violet-400 bg-violet-500/5" } else { "border-transparent text-gray-400 hover:text-gray-200" })
                    on:click=move |_| set_active_tab(MarketTab::Gathering)
                >
                    "Gatherer Routes"
                </button>
                <button
                    class=move || format!("px-5 py-3 font-semibold transition-all duration-300 border-b-2 -mb-[2px] {}",
                        if active_tab() == MarketTab::Settings { "border-violet-500 text-violet-400 bg-violet-500/5" } else { "border-transparent text-gray-400 hover:text-gray-200" })
                    on:click=move |_| set_active_tab(MarketTab::Settings)
                >
                    "Settings"
                </button>
            </div>

            // Active Tab Content
            <div>
                {move || if !is_authenticated() {
                    view! {
                        <div class="max-w-md mx-auto my-12 bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-8 shadow-2xl text-center space-y-6">
                            <div class="mx-auto w-16 h-16 rounded-full bg-violet-500/10 flex items-center justify-center text-violet-400">
                                <Icon icon=i::BiUserRegular attr:class="text-3xl" />
                            </div>
                            <div class="space-y-2">
                                <h2 class="text-xl font-bold text-gray-100">"Authentication Required"</h2>
                                <p class="text-sm text-gray-400">
                                    "Please login with Discord to manage your profile, track your gil balance, find profitable cross-world flips, and analyze crafting/gathering opportunities."
                                </p>
                            </div>
                            <a
                                rel="external"
                                href="/login"
                                class="inline-flex items-center justify-center gap-2 w-full py-3 px-4 bg-gradient-to-r from-violet-600 to-fuchsia-600 hover:from-violet-500 hover:to-fuchsia-500 text-white font-semibold rounded-xl shadow-lg transition-all duration-300 transform hover:scale-[1.02]"
                            >
                                <Icon icon=i::BiLogInRegular />
                                "Login with Discord"
                            </a>
                        </div>
                    }.into_any()
                } else {
                    match (active_profile(), setup_status()) {
                        (None, _) => view! { <CreateFirstProfileView reload_profiles=load_profiles /> }.into_any(),
                        (Some(_), None) => view! {
                            <div class="text-sm text-gray-400 bg-white/5 border border-white/10 rounded-xl p-4">
                                "Checking profile setup..."
                            </div>
                        }.into_any(),
                        (Some(profile), Some(status)) if !status.complete => view! {
                            <FirstTimeSetupView profile=profile status=status reload_profiles=load_profiles />
                        }.into_any(),
                        _ => match active_tab() {
                            MarketTab::Dashboard => view! { <DashboardView profile=active_profile() reload_profiles=load_profiles /> }.into_any(),
                            MarketTab::Arbitrage => view! { <ArbitrageView profile_id=active_profile_id() /> }.into_any(),
                            MarketTab::Crafting => view! { <CraftingView profile_id=active_profile_id() /> }.into_any(),
                            MarketTab::Gathering => view! { <GatheringView profile_id=active_profile_id() /> }.into_any(),
                            MarketTab::Settings => view! { <SettingsView profile=active_profile() profiles=profiles() reload_profiles=load_profiles /> }.into_any(),
                        },
                    }
                }}
            </div>
        </div>
    }
}

#[component]
fn CreateFirstProfileView(
    reload_profiles: impl Fn() + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let (profile_name, set_profile_name) = signal("Main".to_string());
    let (error, set_error) = signal(None::<String>);

    let create = move |_| {
        let name = profile_name().trim().to_string();
        if name.is_empty() {
            set_error(Some("Enter a profile name first.".to_string()));
            return;
        }
        spawn_local(async move {
            match create_profile(name).await {
                Ok(_) => {
                    set_error(None);
                    reload_profiles();
                }
                Err(e) => set_error(Some(e.to_string())),
            }
        });
    };

    view! {
        <div class="max-w-xl mx-auto my-12 bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-8 shadow-2xl space-y-6">
            <div class="space-y-2">
                <h2 class="text-2xl font-bold text-gray-100">"Create your market profile"</h2>
                <p class="text-sm text-gray-400">
                    "Profiles keep worlds, gil balance, alert channels, and market thresholds separate for each character or alt."
                </p>
            </div>
            <div class="space-y-2">
                <label class="block text-gray-400 font-semibold text-sm">"Profile name"</label>
                <input
                    type="text"
                    class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                    prop:value=profile_name
                    on:input=move |ev| set_profile_name(event_target_value(&ev))
                />
            </div>
            {move || error().map(|message| view! {
                <div class="text-sm text-rose-300 bg-rose-500/10 border border-rose-400/20 rounded-xl p-3">{message}</div>
            })}
            <button
                class="inline-flex items-center justify-center gap-2 w-full py-3 px-4 bg-violet-600 hover:bg-violet-500 text-white font-semibold rounded-xl shadow-lg transition-colors"
                on:click=create
            >
                <Icon icon=i::BiPlusRegular />
                "Create Profile"
            </button>
        </div>
    }
}

#[component]
fn FirstTimeSetupView(
    profile: PlayerProfile,
    status: ProfileSetupStatus,
    reload_profiles: impl Fn() + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let world_data = use_context::<LocalWorldData>().map(|data| data.0);
    let profile_id = profile.id;
    let profile_display_name = profile.display_name.clone();
    let (home_world, set_home_world) = signal(profile.home_world_id.map(AnySelector::World));
    let (active_market, set_active_market) =
        signal(profile.active_datacenter_id.map(AnySelector::Datacenter));
    let (gil_balance, set_gil_balance) = signal(profile.gil_balance.max(0));
    let (min_profit, set_min_profit) = signal(25_000i64);
    let (velocity_threshold, set_velocity_threshold) = signal(1.0f64);
    let (travel_rate, set_travel_rate) = signal(10_000i64);
    let (min_profit_total, set_min_profit_total) = signal(50_000i64);
    let (error, set_error) = signal(None::<String>);
    let (saving, set_saving) = signal(false);

    {
        Effect::new(move |_| {
            spawn_local(async move {
                if let Ok(settings) = get_arbitrage_settings(profile_id).await {
                    set_min_profit(settings.min_net_profit.max(25_000));
                    set_velocity_threshold(settings.velocity_threshold.max(1.0));
                    set_travel_rate(settings.travel_cost_rate_per_min.max(10_000));
                    set_min_profit_total(settings.min_profit_total.max(50_000));
                }
            });
        });
    }

    let missing_label = move |key: &str| {
        match key {
            "home_world" => "Home world",
            "active_datacenter" => "Active market data center",
            "gil_balance" => "Gil balance",
            "arbitrage_min_net_profit" => "Minimum net profit",
            "arbitrage_velocity_threshold" => "Velocity threshold",
            "arbitrage_min_profit_total" => "Minimum profit floor",
            other => other,
        }
        .to_string()
    };

    let save_setup = {
        let world_data = world_data.clone();
        move |_| {
            let Some(AnySelector::World(home_world_id)) = home_world() else {
                set_error(Some("Choose a home world.".to_string()));
                return;
            };

            let active_datacenter_id = match active_market() {
                Some(AnySelector::Datacenter(id)) => Some(id),
                Some(AnySelector::World(world_id)) => world_data
                    .as_ref()
                    .and_then(|result| result.as_ref().ok())
                    .and_then(|worlds| worlds.lookup_selector(AnySelector::World(world_id)))
                    .and_then(|result| result.as_world().map(|world| world.datacenter_id)),
                _ => None,
            };

            let Some(active_datacenter_id) = active_datacenter_id else {
                set_error(Some(
                    "Choose an active data center or one of its worlds.".to_string(),
                ));
                return;
            };

            if gil_balance() <= 0 {
                set_error(Some("Enter your current gil balance.".to_string()));
                return;
            }
            if min_profit() <= 0 || velocity_threshold() <= 0.0 || min_profit_total() <= 0 {
                set_error(Some(
                    "Market thresholds must be greater than zero.".to_string(),
                ));
                return;
            }

            set_saving(true);
            set_error(None);
            let gil = gil_balance();
            let min_p = min_profit();
            let velocity = velocity_threshold();
            let travel = travel_rate();
            let min_total = min_profit_total();

            spawn_local(async move {
                let profile_result = update_profile(
                    profile_id,
                    json!({
                        "home_world_id": home_world_id,
                        "active_datacenter_id": active_datacenter_id,
                        "gil_balance": gil,
                    }),
                )
                .await;

                let settings_result = update_arbitrage_settings(
                    profile_id,
                    json!({
                        "min_net_profit": min_p,
                        "velocity_threshold": velocity,
                        "travel_cost_rate_per_min": travel,
                        "min_profit_total": min_total,
                        "category_blocklist": null,
                        "category_allowlist": null,
                        "world_exclusion_list": null,
                        "excluded_item_ids": null,
                        "max_listing_age_hours": 4,
                        "show_stale_panel": false,
                    }),
                )
                .await;

                set_saving(false);
                match (profile_result, settings_result) {
                    (Ok(_), Ok(_)) => reload_profiles(),
                    (Err(e), _) | (_, Err(e)) => set_error(Some(e.to_string())),
                }
            });
        }
    };

    view! {
        <div class="max-w-4xl mx-auto bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl space-y-6">
            <div class="flex flex-col gap-2">
                <div class="inline-flex items-center gap-2 text-amber-300 text-sm font-semibold">
                    <Icon icon=i::BiCogRegular />
                    "First-time setup"
                </div>
                <h2 class="text-2xl font-bold text-gray-100">"Finish setting up " {profile_display_name.clone()}</h2>
                <p class="text-sm text-gray-400 max-w-2xl">
                    "Recommendations stay locked until these basics are set, so the app does not rank flips or crafts using placeholder values."
                </p>
            </div>

            <div class="flex flex-wrap gap-2">
                {status.missing.into_iter().map(|key| view! {
                    <span class="px-2.5 py-1 rounded-lg text-xs font-semibold bg-amber-400/10 text-amber-200 border border-amber-400/20">
                        {missing_label(&key)}
                    </span>
                }).collect::<Vec<_>>()}
            </div>

            <div class="grid md:grid-cols-2 gap-5 text-sm">
                <div class="space-y-2">
                    <label class="block text-gray-300 font-semibold">"Home world"</label>
                    <WorldPicker
                        current_world=home_world.into()
                        set_current_world=set_home_world.into_signal_setter()
                    />
                </div>
                <div class="space-y-2">
                    <label class="block text-gray-300 font-semibold">"Active market data center"</label>
                    <WorldPicker
                        current_world=active_market.into()
                        set_current_world=set_active_market.into_signal_setter()
                    />
                </div>
                <div class="space-y-2">
                    <label class="block text-gray-300 font-semibold">"Gil balance"</label>
                    <input
                        type="number"
                        class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                        prop:value=gil_balance
                        on:input=move |ev| set_gil_balance(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                    />
                </div>
                <div class="space-y-2">
                    <label class="block text-gray-300 font-semibold">"Minimum net profit"</label>
                    <input
                        type="number"
                        class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                        prop:value=min_profit
                        on:input=move |ev| set_min_profit(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                    />
                </div>
                <div class="space-y-2">
                    <label class="block text-gray-300 font-semibold">"Velocity threshold"</label>
                    <input
                        type="number"
                        step="0.1"
                        class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                        prop:value=velocity_threshold
                        on:input=move |ev| set_velocity_threshold(event_target_value(&ev).parse::<f64>().unwrap_or(0.0))
                    />
                </div>
                <div class="space-y-2">
                    <label class="block text-gray-300 font-semibold">"Travel cost rate"</label>
                    <input
                        type="number"
                        class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                        prop:value=travel_rate
                        on:input=move |ev| set_travel_rate(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                    />
                </div>
                <div class="space-y-2 md:col-span-2">
                    <label class="block text-gray-300 font-semibold">"Minimum profit floor"</label>
                    <input
                        type="number"
                        class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                        prop:value=min_profit_total
                        on:input=move |ev| set_min_profit_total(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                    />
                </div>
            </div>

            {move || error().map(|message| view! {
                <div class="text-sm text-rose-300 bg-rose-500/10 border border-rose-400/20 rounded-xl p-3">{message}</div>
            })}

            <div class="flex justify-end">
                <button
                    class="inline-flex items-center justify-center gap-2 px-5 py-2.5 bg-violet-600 hover:bg-violet-500 disabled:bg-zinc-700 disabled:text-zinc-400 text-white font-semibold rounded-xl transition-colors"
                    disabled=saving
                    on:click=save_setup
                >
                    <Icon icon=i::BiCheckRegular />
                    {move || if saving() { "Saving..." } else { "Complete Setup" }}
                </button>
            </div>
        </div>
    }
}

#[component]
fn DashboardView(
    profile: Option<PlayerProfile>,
    reload_profiles: impl Fn() + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let (is_editing_gil, set_is_editing_gil) = signal(false);
    let (gil_input, set_gil_input) = signal(0i64);

    // Local signals for top opportunities
    let (top_arb, set_top_arb) = signal(None::<ArbitrageOpportunity>);
    let (top_craft, set_top_craft) = signal(None::<CraftingOpportunity>);

    let profile_for_effect = profile.clone();
    Effect::new(move |_| {
        if let Some(p) = &profile_for_effect {
            set_gil_input(p.gil_balance);

            // Load top opportunities
            let pid = p.id;
            spawn_local(async move {
                if let Ok(list) = get_arbitrage_opportunities_api(pid).await {
                    if !list.is_empty() {
                        set_top_arb(Some(list[0].clone()));
                    } else {
                        set_top_arb(None);
                    }
                }
                if let Ok(list) = get_crafting_opportunities_api(pid, false).await {
                    if !list.is_empty() {
                        set_top_craft(Some(list[0].clone()));
                    } else {
                        set_top_craft(None);
                    }
                }
            });
        }
    });

    // save_gil has been inlined into button on:click

    let profile_id = profile.as_ref().map(|p| p.id);

    view! {
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            // Gil Balance Card
            <div class="md:col-span-1 bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-lg flex flex-col justify-between">
                <div>
                    <h3 class="text-sm font-semibold uppercase tracking-wider text-gray-400">"Manually Tracked Gil"</h3>
                    <div class="mt-4 flex items-baseline gap-2">
                        {move || if is_editing_gil() {
                            Either::Left(view! {
                                <div class="flex gap-2 w-full">
                                    <input
                                        type="number"
                                        class="p-2 rounded-lg bg-zinc-950/80 border border-violet-500/30 text-2xl font-bold w-full text-violet-300 focus:outline-none"
                                        prop:value=gil_input
                                        on:input=move |ev| set_gil_input(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                                    />
                                    <button
                                        class="px-3 py-1 bg-violet-600 rounded-lg font-semibold hover:bg-violet-500 transition-colors"
                                        on:click={
                                            let reload = reload_profiles;
                                            move |_| {
                                                if let Some(pid) = profile_id {
                                                    let val = gil_input();
                                                    spawn_local(async move {
                                                        if update_profile(pid, json!({ "gil_balance": val })).await.is_ok() {
                                                            set_is_editing_gil(false);
                                                            reload();
                                                        }
                                                    });
                                                }
                                            }
                                        }
                                    >
                                        "Save"
                                    </button>
                                </div>
                            })
                        } else {
                            Either::Right(view! {
                                <span class="text-4xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-amber-200 to-yellow-400">
                                    {move || format!("{} Gil", gil_input().separate_with_commas())}
                                </span>
                            })
                        }}
                    </div>
                </div>

                <div class="mt-6 flex justify-end">
                    {move || if !is_editing_gil() {
                        Some(view! {
                            <button
                                class="text-xs px-3 py-1.5 rounded-lg border border-white/10 hover:border-violet-500/40 text-gray-400 hover:text-violet-300 transition-all"
                                on:click=move |_| set_is_editing_gil(true)
                            >
                                "Update Balance"
                            </button>
                        })
                    } else {
                        None
                    }}
                </div>
            </div>

            // Best Arbitrage Flip Card
            <div class="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-lg">
                <h3 class="text-sm font-semibold uppercase tracking-wider text-gray-400 flex items-center gap-2">
                    <Icon icon=i::BiRefreshRegular attr:class="text-violet-400" />
                    "Top Arbitrage Pick"
                </h3>

                {move || match top_arb() {
                    Some(opp) => Either::Left(view! {
                        <div class="mt-4 space-y-3">
                            <div class="flex items-center gap-3">
                                <ItemIcon item_id=opp.item_id icon_size=IconSize::Small />
                                <div>
                                    <div class="text-xl font-bold text-gray-100">
                                        {tracked_data()
                                            .items
                                            .get(&xiv_gen::ItemId(opp.item_id))
                                            .map(|item| item.name.as_str().to_string())
                                            .unwrap_or_else(|| format!("Item #{}", opp.item_id))}
                                    </div>
                                    <div class="text-xs text-gray-500">
                                        {format!("#{} {}", opp.item_id, if opp.hq { "HQ" } else { "NQ" })}
                                    </div>
                                </div>
                            </div>
                            <div class="grid grid-cols-2 gap-2 text-sm text-gray-400">
                                <div>"Buy From:" <span class="text-gray-200 font-medium"><WorldName id=AnySelector::World(opp.source_world_id) /></span></div>
                                <div>"Sell On:" <span class="text-gray-200 font-medium"><WorldName id=AnySelector::World(opp.dest_world_id) /></span></div>
                                <div>"Est Net Profit:" <span class="text-emerald-400 font-semibold">{format!("{} Gil", opp.net_profit.separate_with_commas())}</span></div>
                                <div>"Velocity:" <span class="text-violet-400 font-semibold">{format!("{:.2}", opp.velocity_score)}</span></div>
                            </div>
                            {opp.over_budget.then(|| view! {
                                <span class="inline-block mt-2 px-2 py-0.5 rounded text-xs font-semibold bg-amber-400/10 text-amber-300 border border-amber-400/20">
                                    "OVER BUDGET"
                                </span>
                            })}
                        </div>
                    }),
                    None => Either::Right(view! {
                        <div class="mt-8 text-center text-sm text-gray-500">
                            "No active flips. Run a scan or adjust settings."
                        </div>
                    })
                }}
            </div>

            // Best Crafting Opportunity Card
            <div class="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-lg">
                <h3 class="text-sm font-semibold uppercase tracking-wider text-gray-400 flex items-center gap-2">
                    <Icon icon=i::BiWrenchRegular attr:class="text-fuchsia-400" />
                    "Top Crafting Pick"
                </h3>

                {move || match top_craft() {
                    Some(opp) => Either::Left(view! {
                        <div class="mt-4 space-y-3">
                            <div class="text-xl font-bold text-gray-100">{opp.name}</div>
                            <div class="grid grid-cols-2 gap-2 text-sm text-gray-400">
                                <div>"Level/Class:" <span class="text-gray-200 font-medium">{format!("{} {}", opp.level, opp.craft_type)}</span></div>
                                <div>"Material Cost:" <span class="text-gray-200 font-medium">{format!("{} Gil", opp.material_cost.separate_with_commas())}</span></div>
                                <div>"Net Profit:" <span class="text-emerald-400 font-semibold">{format!("{} Gil", opp.net_profit.separate_with_commas())}</span></div>
                            </div>
                            <div class="flex flex-wrap gap-1 mt-2">
                                {opp.flags.clone().into_iter().map(|f| view! {
                                    <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-fuchsia-400/10 text-fuchsia-300 border border-fuchsia-400/20">{f}</span>
                                }).collect::<Vec<_>>()}
                            </div>
                        </div>
                    }),
                    None => Either::Right(view! {
                        <div class="mt-8 text-center text-sm text-gray-500">
                            "No profitable recipes calculated."
                        </div>
                    })
                }}
            </div>
        </div>
    }
}

#[component]
fn ArbitrageView(profile_id: Option<i32>) -> impl IntoView {
    let (opportunities, set_opportunities) = signal(Vec::<ArbitrageOpportunity>::new());
    let (loading, set_loading) = signal(true);
    let (load_error, set_load_error) = signal(None::<String>);
    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let (last_refreshed, set_last_refreshed) = signal(None::<String>);

    #[cfg(feature = "hydrate")]
    {
        let interval = send_wrapper::SendWrapper::new(gloo_timers::callback::Interval::new(
            60_000,
            move || {
                set_refresh_tick.update(|tick| *tick = tick.wrapping_add(1));
            },
        ));
        on_cleanup(move || {
            drop(interval);
        });
    }

    // Load flips
    Effect::new(move |_| {
        let _ = refresh_tick();
        if let Some(pid) = profile_id {
            set_loading(true);
            set_load_error(None);
            spawn_local(async move {
                match get_arbitrage_opportunities_api(pid).await {
                    Ok(list) => {
                        set_opportunities(list);
                        set_load_error(None);
                        set_last_refreshed(Some("Updated just now".to_string()));
                    }
                    Err(err) => {
                        set_opportunities(Vec::new());
                        set_load_error(Some(format!("{err:?}")));
                        set_last_refreshed(None);
                    }
                }
                set_loading(false);
            });
        } else {
            set_opportunities(Vec::new());
            set_load_error(Some("No active profile selected.".to_string()));
            set_loading(false);
        }
    });

    view! {
        <div class="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl space-y-6">
            <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <div>
                    <h3 class="text-xl font-bold text-gray-100">"Arbitrage Opportunities"</h3>
                    <div class="mt-1 flex flex-wrap items-center gap-3 text-xs text-gray-400">
                        <span>{move || format!("{} matches", opportunities().len())}</span>
                        <span>{move || last_refreshed().unwrap_or_else(|| "Waiting for first refresh".to_string())}</span>
                        <span>"Auto-refreshes every 60s"</span>
                    </div>
                </div>
                <button
                    class="inline-flex items-center justify-center gap-2 rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-gray-200 transition-all hover:border-violet-500/40 hover:bg-violet-500/10 disabled:cursor-wait disabled:opacity-60"
                    disabled=loading
                    on:click=move |_| set_refresh_tick.update(|tick| *tick = tick.wrapping_add(1))
                >
                    <Icon icon=i::BiRefreshRegular attr:class="size-4 text-violet-300" />
                    "Refresh"
                </button>
            </div>

            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-white/10 text-sm text-left">
                    <thead>
                        <tr class="text-gray-400 font-semibold">
                            <th class="py-3 px-4">"Item"</th>
                            <th class="py-3 px-4">"Buy From"</th>
                            <th class="py-3 px-4">"Sell On"</th>
                            <th class="py-3 px-4">"Buy Price"</th>
                            <th class="py-3 px-4">"Competing Price"</th>
                            <th class="py-3 px-4">"Qty"</th>
                            <th class="py-3 px-4">"Total Cost"</th>
                            <th class="py-3 px-4">"Gross Profit"</th>
                            <th class="py-3 px-4">"Net Profit"</th>
                            <th class="py-3 px-4">"Velocity"</th>
                            <th class="py-3 px-4">"Age"</th>
                            <th class="py-3 px-4">"Flags"</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-white/5">
                        {move || {
                            let opportunities = opportunities();
                            if let Some(error) = load_error() {
                                vec![view! {
                                    <tr>
                                        <td colspan="12" class="py-8 px-4 text-center text-rose-300">
                                            {format!("Could not load arbitrage opportunities: {error}")}
                                        </td>
                                    </tr>
                                }.into_any()]
                            } else if loading() {
                                vec![view! {
                                    <tr>
                                        <td colspan="12" class="py-8 px-4 text-center text-gray-400">
                                            "Loading arbitrage opportunities..."
                                        </td>
                                    </tr>
                                }.into_any()]
                            } else if opportunities.is_empty() {
                                vec![view! {
                                    <tr>
                                        <td colspan="12" class="py-8 px-4 text-center text-gray-400">
                                            "No active flips found yet. The scanner runs after market updates and after profile/settings changes; lowering the velocity or profit thresholds can also reveal more candidates."
                                        </td>
                                    </tr>
                                }.into_any()]
                            } else {
                                opportunities.into_iter().map(|opp| {
                                    let buy_price = if opp.quantity_available > 0 {
                                        opp.total_cost / opp.quantity_available as i64
                                    } else {
                                        0
                                    };
                                    let competing_price = if opp.quantity_available > 0 {
                                        buy_price + (opp.gross_profit / opp.quantity_available as i64)
                                    } else {
                                        0
                                    };
                                    let item_name = tracked_data()
                                        .items
                                        .get(&xiv_gen::ItemId(opp.item_id))
                                        .map(|item| item.name.as_str().to_string())
                                        .unwrap_or_else(|| format!("Item #{}", opp.item_id));
                                    view! {
                                        <tr class="hover:bg-white/5 transition-colors">
                                            <td class="py-3 px-4 font-semibold text-gray-200">
                                                <div class="flex items-center gap-3 min-w-[220px]">
                                                    <ItemIcon item_id=opp.item_id icon_size=IconSize::Small />
                                                    <div>
                                                        <a
                                                            class="text-gray-100 hover:text-violet-300 transition-colors"
                                                            href=format!("/market/item/{}", opp.item_id)
                                                        >
                                                            {item_name}
                                                        </a>
                                                        <div class="text-xs text-gray-500">
                                                            {format!("#{} {}", opp.item_id, if opp.hq { "HQ" } else { "NQ" })}
                                                        </div>
                                                    </div>
                                                </div>
                                            </td>
                                            <td class="py-3 px-4">
                                                <WorldName id=AnySelector::World(opp.source_world_id) />
                                            </td>
                                            <td class="py-3 px-4">
                                                <WorldName id=AnySelector::World(opp.dest_world_id) />
                                            </td>
                                            <td class="py-3 px-4 font-mono text-gray-300">{format!("{} Gil", buy_price.separate_with_commas())}</td>
                                            <td class="py-3 px-4 font-mono text-gray-300">{format!("{} Gil", competing_price.separate_with_commas())}</td>
                                            <td class="py-3 px-4">{opp.quantity_available}</td>
                                            <td class="py-3 px-4 text-gray-300">{format!("{} Gil", opp.total_cost.separate_with_commas())}</td>
                                            <td class="py-3 px-4 text-gray-300">{opp.gross_profit.separate_with_commas()}</td>
                                            <td class="py-3 px-4 text-emerald-400 font-semibold">{opp.net_profit.separate_with_commas()}</td>
                                            <td class="py-3 px-4 font-mono">{format!("{:.2}", opp.velocity_score)}</td>
                                            <td class="py-3 px-4 text-gray-400 font-mono">{format!("{}s", opp.listing_age_seconds)}</td>
                                            <td class="py-3 px-4">
                                                {opp.over_budget.then(|| view! {
                                                    <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-amber-400/10 text-amber-300 border border-amber-400/20">"OVER BUDGET"</span>
                                                })}
                                            </td>
                                        </tr>
                                    }.into_any()
                                }).collect::<Vec<_>>()
                            }
                        }}
                    </tbody>
                </table>
            </div>
        </div>
    }
}

#[component]
fn CraftingView(profile_id: Option<i32>) -> impl IntoView {
    let (opportunities, set_opportunities) = signal(Vec::<CraftingOpportunity>::new());
    let (expanded_recipe, set_expanded_recipe) = signal(None::<i32>);
    let (show_all_levels, set_show_all_levels) = signal(false);

    Effect::new(move |_| {
        if let Some(pid) = profile_id {
            let show_all = show_all_levels();
            spawn_local(async move {
                if let Ok(list) = get_crafting_opportunities_api(pid, show_all).await {
                    set_opportunities(list);
                }
            });
        }
    });

    view! {
        <div class="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl space-y-6">
            <div class="flex flex-wrap items-center justify-between gap-4">
                <h3 class="text-xl font-bold text-gray-100">"Recurse-to-BOM Crafting Optimizer"</h3>
                <label class="inline-flex items-center gap-2 text-sm text-gray-300">
                    <input
                        type="checkbox"
                        class="accent-violet-500"
                        prop:checked=show_all_levels
                        on:change=move |ev| set_show_all_levels(event_target_checked(&ev))
                    />
                    "Show all levels"
                </label>
            </div>

            <div class="space-y-4">
                {move || opportunities().into_iter().map(|opp| {
                    let is_expanded = expanded_recipe() == Some(opp.recipe_id);
                    let rid = opp.recipe_id;
                    view! {
                        <div class="border border-white/5 rounded-xl overflow-hidden hover:border-violet-500/20 transition-all bg-zinc-950/20">
                            <div
                                class="p-4 flex flex-wrap justify-between items-center gap-4 cursor-pointer"
                                on:click=move |_| {
                                    if is_expanded {
                                        set_expanded_recipe(None);
                                    } else {
                                        set_expanded_recipe(Some(rid));
                                    }
                                }
                            >
                                <div class="flex items-center gap-3">
                                    <span class="text-violet-400"><Icon icon=i::BiChevronDownRegular attr:class=if is_expanded { "rotate-180 transition-transform" } else { "transition-transform" } /></span>
                                    <div>
                                        <div class="font-bold text-gray-200">{opp.name.clone()}</div>
                                        <div class="text-xs text-gray-400">{format!("Level {} Class #{}", opp.level, opp.craft_type)}</div>
                                    </div>
                                </div>

                                <div class="flex items-center gap-6 text-sm">
                                    <div>"BOM Material Cost: " <span class="text-gray-300 font-semibold">{format!("{} Gil", opp.material_cost.separate_with_commas())}</span></div>
                                    <div>"Sell Price: " <span class="text-gray-300 font-semibold">{format!("{} Gil", opp.sell_price.separate_with_commas())}</span></div>
                                    <div>"Net Profit: " <span class="text-emerald-400 font-bold">{format!("{} Gil", opp.net_profit.separate_with_commas())}</span></div>
                                    <div class="flex gap-1">
                                        {opp.flags.clone().into_iter().map(|f| view! {
                                            <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-fuchsia-400/10 text-fuchsia-300 border border-fuchsia-400/20">{f}</span>
                                        }).collect::<Vec<_>>()}
                                    </div>
                                </div>
                            </div>

                            // Bill of Materials breakdown
                            {move || is_expanded.then(|| view! {
                                <div class="px-6 pb-4 pt-2 border-t border-white/5 bg-zinc-950/40 text-xs">
                                    <h4 class="font-semibold text-violet-300 uppercase tracking-wider text-[10px] mb-3">"Bill of Materials & Sub-Craft Savings Path"</h4>
                                    <div class="space-y-2">
                                        {opp.ingredients.clone().into_iter().map(|ing| view! {
                                            <div class="flex justify-between items-center py-1 border-b border-white/5">
                                                <div>
                                                    <span class="font-medium text-gray-300">{ing.name.clone()}</span>
                                                    <span class="text-gray-500 font-mono ml-2">"x" {ing.quantity}</span>
                                                </div>
                                                <div class="flex items-center gap-4">
                                                    <span class="text-gray-400 font-mono">{format!("{} Gil/unit", ing.cost_per_unit)}</span>
                                                    <span class=format!("px-2 py-0.5 rounded text-[10px] font-bold {}",
                                                        if ing.path == "Craft" { "bg-emerald-400/10 text-emerald-300 border border-emerald-400/20" } else { "bg-zinc-800 text-zinc-400 border border-zinc-700" })>
                                                        {ing.path.clone()}
                                                    </span>
                                                    <span class="font-semibold text-gray-200 font-mono min-w-[70px] text-right">{format!("{} Gil", ing.total_cost.separate_with_commas())}</span>
                                                </div>
                                            </div>
                                        }).collect::<Vec<_>>()}
                                    </div>
                                </div>
                            })}
                        </div>
                    }
                }).collect::<Vec<_>>()}
            </div>
        </div>
    }
}

#[component]
fn GatheringView(profile_id: Option<i32>) -> impl IntoView {
    let (normal_items, set_normal_items) = signal(Vec::<GatheringNodeDetail>::new());
    let (timed_items, set_timed_items) = signal(Vec::<TimedNodeDetail>::new());
    let (show_all_levels, set_show_all_levels) = signal(false);

    Effect::new(move |_| {
        if let Some(pid) = profile_id {
            let show_all = show_all_levels();
            spawn_local(async move {
                if let Ok((normal, timed)) = get_gathering_routes_api(pid, show_all).await {
                    set_normal_items(normal);
                    set_timed_items(timed);
                }
            });
        }
    });

    view! {
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div class="lg:col-span-2 flex justify-end">
                <label class="inline-flex items-center gap-2 text-sm text-gray-300 bg-white/5 border border-white/10 rounded-xl px-3 py-2">
                    <input
                        type="checkbox"
                        class="accent-violet-500"
                        prop:checked=show_all_levels
                        on:change=move |ev| set_show_all_levels(event_target_checked(&ev))
                    />
                    "Show all levels"
                </label>
            </div>
            // Always-Available Nodes
            <div class="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl space-y-4">
                <h3 class="text-lg font-bold text-transparent bg-clip-text bg-gradient-to-r from-violet-400 to-fuchsia-400">"Always-Available Nodes"</h3>
                <div class="overflow-x-auto">
                    <table class="min-w-full divide-y divide-white/10 text-xs text-left">
                        <thead>
                            <tr class="text-gray-400 font-semibold">
                                <th class="py-2 px-3">"Item Name"</th>
                                <th class="py-2 px-3">"Class"</th>
                                <th class="py-2 px-3">"Level"</th>
                                <th class="py-2 px-3">"Price"</th>
                                <th class="py-2 px-3">"Node Score"</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-white/5">
                            {move || normal_items().into_iter().map(|item| view! {
                                <tr class="hover:bg-white/5 transition-colors">
                                    <td class="py-2 px-3 font-semibold text-gray-200">{item.name.clone()}</td>
                                    <td class="py-2 px-3">{item.class_kind.clone()}</td>
                                    <td class="py-2 px-3 font-mono">{item.level}</td>
                                    <td class="py-2 px-3 text-emerald-400 font-semibold font-mono">{format!("{} Gil", item.unit_price.separate_with_commas())}</td>
                                    <td class="py-2 px-3 text-violet-400 font-bold font-mono">{format!("{:.1}", item.node_score)}</td>
                                </tr>
                            }).collect::<Vec<_>>()}
                        </tbody>
                    </table>
                </div>
            </div>

            // Timed Nodes
            <div class="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl space-y-4">
                <h3 class="text-lg font-bold text-transparent bg-clip-text bg-gradient-to-r from-violet-400 to-fuchsia-400">"Timed Nodes Clock"</h3>
                <div class="overflow-x-auto">
                    <table class="min-w-full divide-y divide-white/10 text-xs text-left">
                        <thead>
                            <tr class="text-gray-400 font-semibold">
                                <th class="py-2 px-3">"Item Name"</th>
                                <th class="py-2 px-3">"Class"</th>
                                <th class="py-2 px-3">"Level"</th>
                                <th class="py-2 px-3">"Next Spawn (Local)"</th>
                                <th class="py-2 px-3">"Window"</th>
                                <th class="py-2 px-3">"Node Score"</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-white/5">
                            {move || timed_items().into_iter().map(|item| view! {
                                <tr class="hover:bg-white/5 transition-colors">
                                    <td class="py-2 px-3 font-semibold text-gray-200">{item.name.clone()}</td>
                                    <td class="py-2 px-3">{item.class_kind.clone()}</td>
                                    <td class="py-2 px-3 font-mono">{item.level}</td>
                                    <td class="py-2 px-3 text-amber-300 font-semibold">{item.next_spawn_local.clone()}</td>
                                    <td class="py-2 px-3 font-mono">{item.duration_hours} "h ET"</td>
                                    <td class="py-2 px-3 text-violet-400 font-bold font-mono">{format!("{:.1}", item.node_score)}</td>
                                </tr>
                            }).collect::<Vec<_>>()}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    }
}
#[component]
fn SettingsView(
    profile: Option<PlayerProfile>,
    profiles: Vec<PlayerProfile>,
    reload_profiles: impl Fn() + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let (profile_name, set_profile_name) = signal("".to_string());

    // Arbitrage Settings inputs
    let (min_profit, set_min_profit) = signal(0i64);
    let (vel_thresh, set_vel_thresh) = signal(0.0f64);
    let (travel_rate, set_travel_rate) = signal(0i64);
    let (min_profit_t, set_min_profit_t) = signal(0i64);
    let (webhook_url, set_webhook_url) = signal("".to_string());

    let profile_for_effect = profile.clone();
    Effect::new(move |_| {
        if let Some(p) = &profile_for_effect {
            set_webhook_url(p.alert_channel_webhook.clone().unwrap_or_default());
            let pid = p.id;
            spawn_local(async move {
                if let Ok(settings) = get_arbitrage_settings(pid).await {
                    set_min_profit(settings.min_net_profit);
                    set_vel_thresh(settings.velocity_threshold);
                    set_travel_rate(settings.travel_cost_rate_per_min);
                    set_min_profit_t(settings.min_profit_total);
                }
            });
        }
    });

    let add_profile = move |_| {
        let name = profile_name();
        if !name.trim().is_empty() {
            spawn_local(async move {
                if create_profile(name).await.is_ok() {
                    set_profile_name("".to_string());
                    reload_profiles();
                }
            });
        }
    };

    let profile_for_save = profile.clone();
    let save_settings = move |_| {
        if let Some(p) = &profile_for_save {
            let pid = p.id;
            let wh = webhook_url();
            let min_p = min_profit();
            let vel = vel_thresh();
            let tr = travel_rate();
            let min_pt = min_profit_t();

            spawn_local(async move {
                let _ = update_profile(pid, json!({ "alert_channel_webhook": Some(wh) })).await;
                let _ = update_arbitrage_settings(
                    pid,
                    json!({
                        "min_net_profit": min_p,
                        "velocity_threshold": vel,
                        "travel_cost_rate_per_min": tr,
                        "min_profit_total": min_pt,
                        "category_blocklist": null,
                        "category_allowlist": null,
                        "world_exclusion_list": null,
                        "excluded_item_ids": null,
                        "max_listing_age_hours": 4,
                        "show_stale_panel": false,
                    }),
                )
                .await;
                reload_profiles();
            });
        }
    };

    view! {
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            // Profiles CRUD
            <div class="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl space-y-6">
                <h3 class="text-lg font-bold text-gray-100">"Profile Management"</h3>
                <div class="flex gap-2">
                    <input
                        type="text"
                        class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                        placeholder="New Profile Name"
                        prop:value=profile_name
                        on:input=move |ev| set_profile_name(event_target_value(&ev))
                    />
                    <button
                        class="px-4 py-2 bg-violet-600 rounded-lg font-semibold hover:bg-violet-500 transition-colors text-sm whitespace-nowrap"
                        on:click=add_profile
                    >
                        "Create Profile"
                    </button>
                </div>

                // Existing profiles list with delete option
                <div class="mt-4 border-t border-white/10 pt-4 space-y-3">
                    <h4 class="text-sm font-semibold text-gray-400">"Existing Profiles"</h4>
                    <div class="space-y-2 max-h-48 overflow-y-auto pr-1">
                        {profiles.clone().into_iter().map(|p| {
                            let pid = p.id;
                            let name = p.display_name.clone();
                            let reload = reload_profiles;
                            view! {
                                <div class="flex justify-between items-center bg-white/5 border border-white/5 rounded-xl p-3 text-sm hover:border-white/10 transition-all">
                                    <span class="text-gray-300 font-medium">{name}</span>
                                    <button
                                        class="text-rose-400 hover:text-rose-300 hover:bg-rose-500/10 rounded-lg p-1.5 transition-all"
                                        title="Delete Profile"
                                        on:click=move |_| {
                                            spawn_local(async move {
                                                if delete_profile(pid).await.is_ok() {
                                                    reload();
                                                }
                                            });
                                        }
                                    >
                                        <Icon icon=i::BiTrashRegular />
                                    </button>
                                </div>
                            }
                        }).collect::<Vec<_>>()}
                    </div>
                </div>
            </div>

            // Arbitrage Settings & Alerts configuration
            <div class="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl space-y-6">
                <h3 class="text-lg font-bold text-gray-100">"Arbitrage Gates & Alerts Settings"</h3>

                <div class="space-y-4 text-sm">
                    <div>
                        <label class="block text-gray-400 font-semibold mb-1">"Discord Alert Webhook URL"</label>
                        <input
                            type="text"
                            class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                            prop:value=webhook_url
                            on:input=move |ev| set_webhook_url(event_target_value(&ev))
                        />
                    </div>

                    <div class="grid grid-cols-2 gap-4">
                        <div>
                            <label class="block text-gray-400 font-semibold mb-1">"Min Net Profit (Gil)"</label>
                            <input
                                type="number"
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=min_profit
                                on:input=move |ev| set_min_profit(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                            />
                        </div>
                        <div>
                            <label class="block text-gray-400 font-semibold mb-1">"Velocity Threshold"</label>
                            <input
                                type="number"
                                step="0.1"
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=vel_thresh
                                on:input=move |ev| set_vel_thresh(event_target_value(&ev).parse::<f64>().unwrap_or(0.0))
                            />
                        </div>
                        <div>
                            <label class="block text-gray-400 font-semibold mb-1">"Travel Cost Rate (Gil/Min)"</label>
                            <input
                                type="number"
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=travel_rate
                                on:input=move |ev| set_travel_rate(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                            />
                        </div>
                        <div>
                            <label class="block text-gray-400 font-semibold mb-1">"Min Profit Floor (Gil)"</label>
                            <input
                                type="number"
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=min_profit_t
                                on:input=move |ev| set_min_profit_t(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                            />
                        </div>
                    </div>

                    <div class="flex justify-end pt-4">
                        <button
                            class="px-5 py-2.5 bg-violet-600 rounded-lg font-bold hover:bg-violet-500 transition-colors"
                            on:click=save_settings
                        >
                            "Save Settings"
                        </button>
                    </div>
                </div>
            </div>
        </div>
    }
}
