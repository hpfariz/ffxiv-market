use crate::api::*;
use crate::components::endpoints_panel::EndpointsPanel;
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
use ultros_api_types::alert::{Endpoint, EndpointMethod};
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
fn SettingHelpLabel(label: &'static str, tooltip: &'static str) -> impl IntoView {
    view! {
        <div class="mb-1 flex items-center gap-1.5">
            <span class="block text-gray-400 font-semibold">{label}</span>
            <span class="group relative inline-flex">
                <span
                    tabindex="0"
                    aria-label=tooltip
                    title=tooltip
                    class="inline-flex h-4 w-4 items-center justify-center rounded-full border border-violet-400/40 bg-violet-500/10 text-[10px] font-bold leading-none text-violet-200 outline-none focus:border-violet-300 focus:bg-violet-500/20"
                >
                    "?"
                </span>
                <span class="pointer-events-none absolute left-1/2 top-full z-30 mt-2 hidden w-64 -translate-x-1/2 rounded-md border border-white/10 bg-zinc-950 p-2 text-xs font-normal leading-snug text-gray-300 shadow-xl group-hover:block group-focus-within:block">
                    {tooltip}
                </span>
            </span>
        </div>
    }
}

fn endpoint_method_label(method: &EndpointMethod) -> &'static str {
    match method {
        EndpointMethod::DiscordDm { .. } => "Discord DM",
        EndpointMethod::DiscordChannel { .. } => "Discord channel",
        EndpointMethod::Webhook { .. } => "Discord webhook",
        EndpointMethod::WebPush { .. } => "Browser push",
    }
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
                Err(crate::error::AppError::ApiError(
                    ultros_api_types::result::ApiError::NotAuthenticated,
                )) => {
                    set_is_authenticated(false);
                }
                Err(e) => {
                    log::error!("Error loading profiles: {e:?}");
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
    let (eorzea_time_str, _set_eorzea_time_str) = signal("00:00".to_string());

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
                _set_eorzea_time_str(format!("{:02}:{:02} ET", hour, minute));
            }));
        on_cleanup(move || {
            drop(interval);
        });
    }

    // Health state from SSE
    let (health_status, _set_health_status) = signal("Connecting...".to_string());
    let (health_color, _set_health_color) = signal("text-amber-400 bg-amber-400/10".to_string());

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
                                _set_health_status("Healthy".to_string());
                                _set_health_color(
                                    "text-emerald-400 bg-emerald-400/10 border-emerald-400/20"
                                        .to_string(),
                                );
                            } else if let Some(lag) = event_type.get("Lagging") {
                                if let Some(sec) = lag.get("lag_seconds").and_then(|v| v.as_i64()) {
                                    _set_health_status(format!("Lagging ({}m)", sec / 60));
                                } else {
                                    _set_health_status("Lagging".to_string());
                                }
                                _set_health_color(
                                    "text-amber-400 bg-amber-400/10 border-amber-400/20"
                                        .to_string(),
                                );
                            } else if event_type.as_str() == Some("Disconnected") {
                                _set_health_status("Disconnected".to_string());
                                _set_health_color(
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
    let (status_tick, set_status_tick) = signal(0u32);
    let (scan_status, set_scan_status) = signal(None::<ArbitrageScanStatus>);
    let (last_seen_scan_completion, set_last_seen_scan_completion) = signal(None::<String>);
    let (last_refreshed, set_last_refreshed) = signal(None::<String>);

    #[cfg(feature = "hydrate")]
    {
        let table_interval = send_wrapper::SendWrapper::new(gloo_timers::callback::Interval::new(
            60_000,
            move || {
                set_refresh_tick.update(|tick| *tick = tick.wrapping_add(1));
            },
        ));
        let status_interval = send_wrapper::SendWrapper::new(gloo_timers::callback::Interval::new(
            5_000,
            move || {
                set_status_tick.update(|tick| *tick = tick.wrapping_add(1));
            },
        ));
        on_cleanup(move || {
            drop(table_interval);
            drop(status_interval);
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

    Effect::new(move |_| {
        let _ = status_tick();
        if let Some(pid) = profile_id {
            spawn_local(async move {
                if let Ok(status) = get_arbitrage_scan_status_api(pid).await {
                    if status.phase == "complete"
                        && let Some(completed_at) = status.completed_at.clone()
                        && last_seen_scan_completion() != Some(completed_at.clone())
                    {
                        set_last_seen_scan_completion(Some(completed_at));
                        set_refresh_tick.update(|tick| *tick = tick.wrapping_add(1));
                    }
                    set_scan_status(Some(status));
                }
            });
        } else {
            set_scan_status(None);
        }
    });

    let request_scan = move |_| {
        if let Some(pid) = profile_id {
            set_scan_status(Some(ArbitrageScanStatus {
                phase: "queued".to_string(),
                message: "Manual refresh requested; scanner queued".to_string(),
                progress_percent: 5,
                profiles_scanned: 0,
                profiles_total: 0,
                queued_at: None,
                started_at: None,
                completed_at: None,
                last_error: None,
            }));
            spawn_local(async move {
                if let Ok(status) = trigger_arbitrage_scan_api(pid).await {
                    set_scan_status(Some(status));
                    set_status_tick.update(|tick| *tick = tick.wrapping_add(1));
                }
                set_refresh_tick.update(|tick| *tick = tick.wrapping_add(1));
            });
        } else {
            set_refresh_tick.update(|tick| *tick = tick.wrapping_add(1));
        }
    };

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
                    on:click=request_scan
                >
                    <Icon icon=i::BiRefreshRegular attr:class="size-4 text-violet-300" />
                    "Refresh Scan"
                </button>
            </div>

            <Show
                when=move || scan_status().is_some()
                fallback=|| ().into_any()
            >
                {move || {
                    scan_status()
                        .map(|status| {
                            let phase_class = match status.phase.as_str() {
                                "failed" => "text-red-300",
                                "complete" => "text-emerald-300",
                                "queued" | "scanning" => "text-violet-200",
                                _ => "text-gray-300",
                            };
                            let detail = if status.profiles_total > 0 {
                                format!(
                                    "{} of {} profiles",
                                    status.profiles_scanned, status.profiles_total
                                )
                            } else {
                                status.phase.clone()
                            };
                            let bar_class = match status.phase.as_str() {
                                "failed" => "h-full rounded-full bg-red-400 transition-all duration-500",
                                "complete" => "h-full rounded-full bg-emerald-400 transition-all duration-500",
                                _ => "h-full rounded-full bg-violet-400 transition-all duration-500",
                            };
                            let progress_percent = status.progress_percent;
                            let progress_label = format!("{progress_percent}%");
                            let progress_style = format!("width: {progress_percent}%;");
                            let message = status.message.clone();
                            let error_message = status.last_error.clone().unwrap_or_default();
                            let has_error = !error_message.is_empty();
                            view! {
                                <div class="space-y-2 rounded-lg border border-white/10 bg-zinc-950/40 p-3">
                                    <div class="flex flex-col gap-1 sm:flex-row sm:items-center sm:justify-between">
                                        <div class="text-sm">
                                            <span class=format!("font-semibold {phase_class}")>{message}</span>
                                            <span class="ml-2 text-xs text-gray-500">{detail}</span>
                                        </div>
                                        <div class="text-xs text-gray-500">
                                            {progress_label}
                                        </div>
                                    </div>
                                    <div class="h-2 overflow-hidden rounded-full bg-white/10">
                                        <div
                                            class=bar_class
                                            style=progress_style
                                        ></div>
                                    </div>
                                    <Show
                                        when=move || has_error
                                        fallback=|| ().into_any()
                                    >
                                        <div class="text-xs text-red-300">
                                            {error_message.clone()}
                                        </div>
                                    </Show>
                                </div>
                            }.into_any()
                        })
                }}
            </Show>

            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-white/10 text-sm text-left">
                    <thead>
                        <tr class="text-gray-400 font-semibold">
                            <th class="py-3 px-4" title="Item name, item ID, and quality for the flip candidate.">"Item"</th>
                            <th class="py-3 px-4" title="World where the buy-side listing was found.">"Buy From"</th>
                            <th class="py-3 px-4" title="Destination world where the item is expected to sell.">"Sell On"</th>
                            <th class="py-3 px-4" title="Lowest source ask price per unit.">"Buy Price"</th>
                            <th class="py-3 px-4" title="Destination sell-side reference price, using the safer value from current asks and recent sale history.">"Competing Price"</th>
                            <th class="py-3 px-4" title="Quantity available at the source listing price.">"Qty"</th>
                            <th class="py-3 px-4" title="Buy price multiplied by quantity.">"Total Cost"</th>
                            <th class="py-3 px-4" title="Estimated profit before travel or time deduction.">"Gross Profit"</th>
                            <th class="py-3 px-4" title="Estimated profit after travel or time deduction.">"Net Profit"</th>
                            <th class="py-3 px-4" title="Current velocity and weekly average velocity. Current velocity is recent sold units divided by active destination listings; weekly average is sales over the last 7 days divided by 7.">"Velocity"</th>
                            <th class="py-3 px-4" title="Estimated travel friction tier for buying and selling this flip.">"Travel"</th>
                            <th class="py-3 px-4" title="Volatility review status based on recent sale regime changes and ask confirmation.">"Risk"</th>
                            <th class="py-3 px-4" title="Age of the source listing used for the flip.">"Age"</th>
                            <th class="py-3 px-4" title="Additional execution flags, such as over-budget warnings.">"Flags"</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-white/5">
                        {move || {
                            let opportunities = opportunities();
                            if let Some(error) = load_error() {
                                vec![view! {
                                    <tr>
                                        <td colspan="14" class="py-8 px-4 text-center text-rose-300">
                                            {format!("Could not load arbitrage opportunities: {error}")}
                                        </td>
                                    </tr>
                                }.into_any()]
                            } else if loading() {
                                vec![view! {
                                    <tr>
                                        <td colspan="14" class="py-8 px-4 text-center text-gray-400">
                                            "Loading arbitrage opportunities..."
                                        </td>
                                    </tr>
                                }.into_any()]
                            } else if opportunities.is_empty() {
                                vec![view! {
                                    <tr>
                                        <td colspan="14" class="py-8 px-4 text-center text-gray-400">
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
                                    let competing_price = if opp.selected_sell_reference_price > 0 {
                                        opp.selected_sell_reference_price as i64
                                    } else if opp.quantity_available > 0 {
                                        buy_price + (opp.gross_profit / opp.quantity_available as i64)
                                    } else {
                                        0
                                    };
                                    let item_name = tracked_data()
                                        .items
                                        .get(&xiv_gen::ItemId(opp.item_id))
                                        .map(|item| item.name.as_str().to_string())
                                        .unwrap_or_else(|| format!("Item #{}", opp.item_id));
                                    let travel_label = match opp.travel_tier.as_str() {
                                        "HOME" => "Home",
                                        "SAME_DC_VISIT" => "Same DC",
                                        "CROSS_DC_TRAVEL" => "Cross DC",
                                        _ => "Unknown",
                                    };
                                    let travel_class = match opp.travel_tier.as_str() {
                                        "HOME" => "bg-emerald-400/10 text-emerald-300 border-emerald-400/20",
                                        "SAME_DC_VISIT" => "bg-sky-400/10 text-sky-300 border-sky-400/20",
                                        "CROSS_DC_TRAVEL" => "bg-amber-400/10 text-amber-300 border-amber-400/20",
                                        _ => "bg-zinc-700/40 text-zinc-300 border-zinc-600",
                                    };
                                    let is_volatile = opp.volatility_flag != "NONE";
                                    let risk_label = match opp.volatility_flag.as_str() {
                                        "UNCONFIRMED_SPIKE" => "Review",
                                        "CONFIRMED_REGIME_CHANGE" => "Regime",
                                        _ => "Clean",
                                    };
                                    let risk_class = match opp.volatility_flag.as_str() {
                                        "UNCONFIRMED_SPIKE" => "bg-amber-400/10 text-amber-300 border-amber-400/20",
                                        "CONFIRMED_REGIME_CHANGE" => "bg-orange-400/10 text-orange-300 border-orange-400/20",
                                        _ => "bg-emerald-400/10 text-emerald-300 border-emerald-400/20",
                                    };
                                    let jump_text = opp.price_jump_ratio
                                        .map(|ratio| format!("Jump {:.0}%", (ratio - 1.0) * 100.0))
                                        .unwrap_or_else(|| "No jump".to_string());
                                    let ask_gap_text = opp.ask_vs_recent_sale_gap_pct
                                        .map(|gap| format!("ask gap {:.1}%", gap))
                                        .unwrap_or_else(|| "ask gap unknown".to_string());
                                    let risk_title = format!(
                                        "{}: recent {} sales avg {} vs prior {} sales avg {}; {}; recent CV {}; prior CV {}; {}",
                                        opp.volatility_flag,
                                        opp.recent_cluster_sales_count,
                                        opp.recent_cluster_avg_price
                                            .map(|price| format!("{:.0}", price))
                                            .unwrap_or_else(|| "n/a".to_string()),
                                        opp.prior_cluster_sales_count,
                                        opp.prior_cluster_avg_price
                                            .map(|price| format!("{:.0}", price))
                                            .unwrap_or_else(|| "n/a".to_string()),
                                        jump_text,
                                        opp.within_cluster_cv_recent
                                            .map(|cv| format!("{:.2}", cv))
                                            .unwrap_or_else(|| "n/a".to_string()),
                                        opp.within_cluster_cv_prior
                                            .map(|cv| format!("{:.2}", cv))
                                            .unwrap_or_else(|| "n/a".to_string()),
                                        ask_gap_text
                                    );
                                    let velocity_title = format!(
                                        "Current velocity = units sold in the last 48h divided by active destination listings ({} units / active listings). Weekly average = total units sold in the last 7 days divided by 7 ({} units / 7).",
                                        opp.units_sold_48h,
                                        opp.units_sold_7d
                                    );
                                    let price_title = format!(
                                        "Destination low ask: {} Gil. Selected sell reference: {} Gil. Median sale: {} Gil. Reference min: {}. Reference avg: {}. Markdown: {}.",
                                        opp.dest_low_ask_price.separate_with_commas(),
                                        opp.selected_sell_reference_price.separate_with_commas(),
                                        opp.median_sale_price.separate_with_commas(),
                                        opp.reference_min_price
                                            .map(|price| format!("{} Gil", price.separate_with_commas()))
                                            .unwrap_or_else(|| "n/a".to_string()),
                                        opp.reference_avg_price
                                            .map(|price| format!("{price:.0} Gil"))
                                            .unwrap_or_else(|| "n/a".to_string()),
                                        opp.markdown_pct
                                            .map(|pct| format!("{pct:.1}%"))
                                            .unwrap_or_else(|| "n/a".to_string())
                                    );
                                    let travel_title = format!(
                                        "{}; estimated travel {} minutes",
                                        opp.execution_status,
                                        opp.travel_minutes
                                    );
                                    view! {
                                        <tr class=if is_volatile { "bg-amber-500/[0.04] hover:bg-amber-500/[0.08] transition-colors" } else { "hover:bg-white/5 transition-colors" }>
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
                                            <td class="py-3 px-4 font-mono text-gray-300" title=price_title>{format!("{} Gil", competing_price.separate_with_commas())}</td>
                                            <td class="py-3 px-4">{opp.quantity_available}</td>
                                            <td class="py-3 px-4 text-gray-300">{format!("{} Gil", opp.total_cost.separate_with_commas())}</td>
                                            <td class="py-3 px-4 text-gray-300">{opp.gross_profit.separate_with_commas()}</td>
                                            <td class="py-3 px-4 text-emerald-400 font-semibold">{opp.net_profit.separate_with_commas()}</td>
                                            <td class="py-3 px-4 font-mono" title=velocity_title>
                                                {format!("{:.2} / {:.1}/day", opp.velocity_score, opp.weekly_avg_velocity)}
                                            </td>
                                            <td class="py-3 px-4" title=travel_title>
                                                <span class=format!("px-2 py-0.5 rounded text-[10px] font-bold border {}", travel_class)>
                                                    {travel_label}
                                                </span>
                                            </td>
                                            <td class="py-3 px-4">
                                                <span
                                                    class=format!("px-2 py-0.5 rounded text-[10px] font-bold border {}", risk_class)
                                                    title=risk_title
                                                >
                                                    {risk_label}
                                                </span>
                                                <Show
                                                    when=move || is_volatile
                                                    fallback=|| ().into_any()
                                                >
                                                    <div class="mt-1 text-[10px] text-amber-200/80 font-mono whitespace-nowrap">
                                                        {jump_text.clone()}
                                                    </div>
                                                </Show>
                                            </td>
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
    let (loading, set_loading) = signal(true);
    let (load_error, set_load_error) = signal(None::<String>);

    Effect::new(move |_| {
        if let Some(pid) = profile_id {
            let show_all = show_all_levels();
            set_loading(true);
            set_load_error(None);
            spawn_local(async move {
                match get_crafting_opportunities_api(pid, show_all).await {
                    Ok(list) => {
                        set_opportunities(list);
                        set_load_error(None);
                    }
                    Err(err) => {
                        set_opportunities(Vec::new());
                        set_load_error(Some(format!("{err:?}")));
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
                {move || {
                    let opportunities = opportunities();
                    if let Some(error) = load_error() {
                        vec![view! {
                            <div class="py-8 text-center text-sm text-rose-300">
                                {format!("Could not load crafting opportunities: {error}")}
                            </div>
                        }.into_any()]
                    } else if loading() {
                        vec![view! {
                            <div class="py-8 text-center text-sm text-gray-400">
                                "Loading crafting opportunities..."
                            </div>
                        }.into_any()]
                    } else if opportunities.is_empty() {
                        vec![view! {
                            <div class="py-8 text-center text-sm text-gray-400">
                                "No crafting opportunities match the current profile and filters yet."
                            </div>
                        }.into_any()]
                    } else {
                        opportunities.into_iter().map(|opp| {
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
                            }.into_any()
                        }).collect::<Vec<_>>()
                    }
                }}
            </div>
        </div>
    }
}

#[component]
fn GatheringView(profile_id: Option<i32>) -> impl IntoView {
    let (normal_items, set_normal_items) = signal(Vec::<GatheringNodeDetail>::new());
    let (timed_items, set_timed_items) = signal(Vec::<TimedNodeDetail>::new());
    let (show_all_levels, set_show_all_levels) = signal(false);
    let (loading, set_loading) = signal(true);
    let (load_error, set_load_error) = signal(None::<String>);

    Effect::new(move |_| {
        if let Some(pid) = profile_id {
            let show_all = show_all_levels();
            set_loading(true);
            set_load_error(None);
            spawn_local(async move {
                match get_gathering_routes_api(pid, show_all).await {
                    Ok((normal, timed)) => {
                        set_normal_items(normal);
                        set_timed_items(timed);
                        set_load_error(None);
                    }
                    Err(err) => {
                        set_normal_items(Vec::new());
                        set_timed_items(Vec::new());
                        set_load_error(Some(format!("{err:?}")));
                    }
                }
                set_loading(false);
            });
        } else {
            set_normal_items(Vec::new());
            set_timed_items(Vec::new());
            set_load_error(Some("No active profile selected.".to_string()));
            set_loading(false);
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
                                <th class="py-2 px-3" title="Gatherable item returned by the optimizer.">"Item Name"</th>
                                <th class="py-2 px-3" title="Gathering class that can collect the item.">"Class"</th>
                                <th class="py-2 px-3" title="Recommended or required gathering level for the item.">"Level"</th>
                                <th class="py-2 px-3" title="Current market price used by the optimizer.">"Price"</th>
                                <th class="py-2 px-3" title="Optimizer score combining market value and node availability.">"Node Score"</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-white/5">
                            {move || {
                                let items = normal_items();
                                if let Some(error) = load_error() {
                                    vec![view! {
                                        <tr><td colspan="5" class="py-6 px-3 text-center text-rose-300">{format!("Could not load routes: {error}")}</td></tr>
                                    }.into_any()]
                                } else if loading() {
                                    vec![view! {
                                        <tr><td colspan="5" class="py-6 px-3 text-center text-gray-400">"Loading gathering routes..."</td></tr>
                                    }.into_any()]
                                } else if items.is_empty() {
                                    vec![view! {
                                        <tr><td colspan="5" class="py-6 px-3 text-center text-gray-400">"No always-available nodes match the current profile and filters."</td></tr>
                                    }.into_any()]
                                } else {
                                    items.into_iter().map(|item| view! {
                                        <tr class="hover:bg-white/5 transition-colors">
                                            <td class="py-2 px-3 font-semibold text-gray-200">{item.name.clone()}</td>
                                            <td class="py-2 px-3">{item.class_kind.clone()}</td>
                                            <td class="py-2 px-3 font-mono">{item.level}</td>
                                            <td class="py-2 px-3 text-emerald-400 font-semibold font-mono">{format!("{} Gil", item.unit_price.separate_with_commas())}</td>
                                            <td class="py-2 px-3 text-violet-400 font-bold font-mono">{format!("{:.1}", item.node_score)}</td>
                                        </tr>
                                    }.into_any()).collect::<Vec<_>>()
                                }
                            }}
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
                                <th class="py-2 px-3" title="Timed-node item returned by the optimizer.">"Item Name"</th>
                                <th class="py-2 px-3" title="Gathering class that can collect the timed item.">"Class"</th>
                                <th class="py-2 px-3" title="Recommended or required gathering level for the timed item.">"Level"</th>
                                <th class="py-2 px-3" title="Next local-time spawn window for the timed node.">"Next Spawn (Local)"</th>
                                <th class="py-2 px-3" title="Duration or time range when the node is available.">"Window"</th>
                                <th class="py-2 px-3" title="Optimizer score combining market value, availability, and timing.">"Node Score"</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-white/5">
                            {move || {
                                let items = timed_items();
                                if let Some(error) = load_error() {
                                    vec![view! {
                                        <tr><td colspan="6" class="py-6 px-3 text-center text-rose-300">{format!("Could not load timed nodes: {error}")}</td></tr>
                                    }.into_any()]
                                } else if loading() {
                                    vec![view! {
                                        <tr><td colspan="6" class="py-6 px-3 text-center text-gray-400">"Loading timed nodes..."</td></tr>
                                    }.into_any()]
                                } else if items.is_empty() {
                                    vec![view! {
                                        <tr><td colspan="6" class="py-6 px-3 text-center text-gray-400">"No timed nodes match the current profile and filters."</td></tr>
                                    }.into_any()]
                                } else {
                                    items.into_iter().map(|item| view! {
                                        <tr class="hover:bg-white/5 transition-colors">
                                            <td class="py-2 px-3 font-semibold text-gray-200">{item.name.clone()}</td>
                                            <td class="py-2 px-3">{item.class_kind.clone()}</td>
                                            <td class="py-2 px-3 font-mono">{item.level}</td>
                                            <td class="py-2 px-3 text-amber-300 font-semibold">{item.next_spawn_local.clone()}</td>
                                            <td class="py-2 px-3 font-mono">{item.duration_hours} "h ET"</td>
                                            <td class="py-2 px-3 text-violet-400 font-bold font-mono">{format!("{:.1}", item.node_score)}</td>
                                        </tr>
                                    }.into_any()).collect::<Vec<_>>()
                                }
                            }}
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
    let (require_home_sell, set_require_home_sell) = signal(true);
    let (source_scope, set_source_scope) = signal("SAME_DC".to_string());
    let (max_price_jump_ratio, set_max_price_jump_ratio) = signal(1.30f64);
    let (min_recent_cluster_confirmations, set_min_recent_cluster_confirmations) = signal(5i32);
    let (volatility_action, set_volatility_action) = signal("DEMOTE_TO_REVIEW".to_string());
    let (require_ask_confirmation, set_require_ask_confirmation) = signal(true);
    let (max_ask_vs_sale_gap_percent, set_max_ask_vs_sale_gap_percent) = signal(15.0f64);
    let (destination_scope, set_destination_scope) = signal("HOME_WORLD".to_string());
    let (weekly_velocity_threshold, set_weekly_velocity_threshold) = signal(0.0f64);
    let (same_dc_travel_minutes, set_same_dc_travel_minutes) = signal(2i32);
    let (cross_dc_travel_minutes, set_cross_dc_travel_minutes) = signal(8i32);
    let (reference_price_scope, set_reference_price_scope) = signal("DESTINATION_DC".to_string());
    let (sell_price_strategy, set_sell_price_strategy) =
        signal("LOWER_OF_ASK_AND_MEDIAN".to_string());
    let (min_markdown_pct, set_min_markdown_pct) = signal(0.0f64);
    let (digest_changed_only, set_digest_changed_only) = signal(true);
    let (digest_max_clean, set_digest_max_clean) = signal(8i32);
    let (digest_max_review, set_digest_max_review) = signal(4i32);
    let (digest_include_review, set_digest_include_review) = signal(true);
    let (digest_include_universalis_links, set_digest_include_universalis_links) = signal(true);
    let (digest_include_ultros_links, set_digest_include_ultros_links) = signal(true);
    let (table_grouping_strategy, set_table_grouping_strategy) =
        signal("BEST_PLUS_SAME_DC".to_string());
    let (table_max_rows_per_item, set_table_max_rows_per_item) = signal(2i32);
    let (table_include_same_dc_best, set_table_include_same_dc_best) = signal(true);
    let (table_show_theoretical, set_table_show_theoretical) = signal(false);
    let (alert_grouping_strategy, set_alert_grouping_strategy) =
        signal("BEST_PLUS_SAME_DC".to_string());
    let (alert_max_rows_per_item, set_alert_max_rows_per_item) = signal(2i32);
    let (alert_include_same_dc_best, set_alert_include_same_dc_best) = signal(true);
    let (alert_show_theoretical, set_alert_show_theoretical) = signal(false);
    let (alert_profit_improvement_threshold_gil, set_alert_profit_improvement_threshold_gil) =
        signal(1i64);
    let (alert_profit_improvement_threshold_pct, set_alert_profit_improvement_threshold_pct) =
        signal(0.0f64);
    let (alert_frequency_mode, set_alert_frequency_mode) = signal("DIGEST_INTERVAL".to_string());
    let (alert_digest_interval_minutes, set_alert_digest_interval_minutes) = signal(60i32);
    let (alert_schedule_cron, set_alert_schedule_cron) = signal("".to_string());
    let (alert_send_empty_digest, set_alert_send_empty_digest) = signal(false);
    let (alert_immediate_threshold_enabled, set_alert_immediate_threshold_enabled) = signal(true);
    let (alert_immediate_min_net_profit, set_alert_immediate_min_net_profit) = signal(500_000i64);
    let (alert_immediate_min_markdown_pct, set_alert_immediate_min_markdown_pct) = signal(0.0f64);
    let (alert_immediate_min_velocity, set_alert_immediate_min_velocity) = signal(0.0f64);
    let (alert_immediate_max_per_hour, set_alert_immediate_max_per_hour) = signal(3i32);
    let (arbitrage_endpoints, set_arbitrage_endpoints) = signal(Vec::<Endpoint>::new());
    let (selected_arbitrage_endpoint_ids, set_selected_arbitrage_endpoint_ids) =
        signal(Vec::<i32>::new());
    let (seller_world_ids_text, set_seller_world_ids_text) = signal("".to_string());
    let (arbitrage_alert_status, set_arbitrage_alert_status) =
        signal(None::<ArbitrageAlertStatusResponse>);
    let (arbitrage_preview, set_arbitrage_preview) = signal(None::<ArbitrageDigestPreview>);
    let (arbitrage_test_result, set_arbitrage_test_result) =
        signal(None::<ArbitrageTestSendResponse>);

    let profile_for_effect = profile.clone();
    Effect::new(move |_| {
        if let Some(p) = &profile_for_effect {
            let pid = p.id;
            spawn_local(async move {
                if let Ok(settings) = get_arbitrage_settings(pid).await {
                    set_min_profit(settings.min_net_profit);
                    set_vel_thresh(settings.velocity_threshold);
                    set_travel_rate(settings.travel_cost_rate_per_min);
                    set_min_profit_t(settings.min_profit_total);
                    set_require_home_sell(settings.require_home_world_sell_target);
                    set_source_scope(settings.source_world_scope);
                    set_max_price_jump_ratio(settings.max_price_jump_ratio);
                    set_min_recent_cluster_confirmations(settings.min_recent_cluster_confirmations);
                    set_volatility_action(settings.volatility_action);
                    set_require_ask_confirmation(settings.require_ask_confirmation);
                    set_max_ask_vs_sale_gap_percent(settings.max_ask_vs_sale_gap_percent);
                    set_destination_scope(settings.destination_world_scope);
                    set_weekly_velocity_threshold(settings.weekly_velocity_threshold);
                    set_same_dc_travel_minutes(settings.same_dc_travel_minutes);
                    set_cross_dc_travel_minutes(settings.cross_dc_travel_minutes);
                    set_reference_price_scope(settings.reference_price_scope);
                    set_sell_price_strategy(settings.sell_price_strategy);
                    set_min_markdown_pct(settings.min_markdown_pct);
                    set_digest_changed_only(settings.digest_changed_only);
                    set_digest_max_clean(settings.digest_max_clean);
                    set_digest_max_review(settings.digest_max_review);
                    set_digest_include_review(settings.digest_include_review);
                    set_digest_include_universalis_links(settings.digest_include_universalis_links);
                    set_digest_include_ultros_links(settings.digest_include_ultros_links);
                    set_table_grouping_strategy(settings.table_grouping_strategy);
                    set_table_max_rows_per_item(settings.table_max_rows_per_item);
                    set_table_include_same_dc_best(settings.table_include_same_dc_best);
                    set_table_show_theoretical(settings.table_show_theoretical);
                    set_alert_grouping_strategy(settings.alert_grouping_strategy);
                    set_alert_max_rows_per_item(settings.alert_max_rows_per_item);
                    set_alert_include_same_dc_best(settings.alert_include_same_dc_best);
                    set_alert_show_theoretical(settings.alert_show_theoretical);
                    set_alert_profit_improvement_threshold_gil(
                        settings.alert_profit_improvement_threshold_gil,
                    );
                    set_alert_profit_improvement_threshold_pct(
                        settings.alert_profit_improvement_threshold_pct,
                    );
                    set_alert_frequency_mode(settings.alert_frequency_mode);
                    set_alert_digest_interval_minutes(settings.alert_digest_interval_minutes);
                    set_alert_schedule_cron(settings.alert_schedule_cron.unwrap_or_default());
                    set_alert_send_empty_digest(settings.alert_send_empty_digest);
                    set_alert_immediate_threshold_enabled(
                        settings.alert_immediate_threshold_enabled,
                    );
                    set_alert_immediate_min_net_profit(settings.alert_immediate_min_net_profit);
                    set_alert_immediate_min_markdown_pct(settings.alert_immediate_min_markdown_pct);
                    set_alert_immediate_min_velocity(settings.alert_immediate_min_velocity);
                    set_alert_immediate_max_per_hour(settings.alert_immediate_max_per_hour);
                    let seller_ids = settings
                        .seller_world_ids
                        .and_then(|value| serde_json::from_value::<Vec<i32>>(value).ok())
                        .unwrap_or_default()
                        .into_iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    set_seller_world_ids_text(seller_ids);
                }
                if let Ok(endpoints) = list_endpoints().await {
                    set_arbitrage_endpoints(endpoints);
                }
                if let Ok(endpoint_ids) = get_arbitrage_destination_endpoint_ids(pid).await {
                    set_selected_arbitrage_endpoint_ids(endpoint_ids);
                }
                if let Ok(status) = get_arbitrage_alert_status_api(pid).await {
                    set_arbitrage_alert_status(Some(status));
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
            let min_p = min_profit();
            let vel = vel_thresh();
            let tr = travel_rate();
            let min_pt = min_profit_t();
            let require_home = require_home_sell();
            let scope = source_scope();
            let jump_ratio = max_price_jump_ratio();
            let confirmations = min_recent_cluster_confirmations();
            let vol_action = volatility_action();
            let require_ask = require_ask_confirmation();
            let ask_gap = max_ask_vs_sale_gap_percent();
            let destination = destination_scope();
            let weekly_vel = weekly_velocity_threshold();
            let same_dc_minutes = same_dc_travel_minutes();
            let cross_dc_minutes = cross_dc_travel_minutes();
            let ref_scope = reference_price_scope();
            let sell_strategy = sell_price_strategy();
            let min_markdown = min_markdown_pct();
            let digest_changed = digest_changed_only();
            let max_clean = digest_max_clean();
            let max_review = digest_max_review();
            let include_review = digest_include_review();
            let include_universalis = digest_include_universalis_links();
            let include_ultros = digest_include_ultros_links();
            let table_grouping = table_grouping_strategy();
            let table_max_rows = table_max_rows_per_item();
            let table_same_dc = table_include_same_dc_best();
            let table_theoretical = table_show_theoretical();
            let alert_grouping = alert_grouping_strategy();
            let alert_max_rows = alert_max_rows_per_item();
            let alert_same_dc = alert_include_same_dc_best();
            let alert_theoretical = alert_show_theoretical();
            let improvement_gil = alert_profit_improvement_threshold_gil();
            let improvement_pct = alert_profit_improvement_threshold_pct();
            let frequency_mode = alert_frequency_mode();
            let interval_minutes = alert_digest_interval_minutes();
            let schedule_cron = alert_schedule_cron();
            let send_empty = alert_send_empty_digest();
            let immediate_enabled = alert_immediate_threshold_enabled();
            let immediate_profit = alert_immediate_min_net_profit();
            let immediate_markdown = alert_immediate_min_markdown_pct();
            let immediate_velocity = alert_immediate_min_velocity();
            let immediate_max_hour = alert_immediate_max_per_hour();
            let endpoint_ids = selected_arbitrage_endpoint_ids();
            let seller_ids = seller_world_ids_text()
                .split(',')
                .filter_map(|part| part.trim().parse::<i32>().ok())
                .collect::<Vec<_>>();

            spawn_local(async move {
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
                        "require_home_world_sell_target": require_home,
                        "source_world_scope": scope,
                        "max_price_jump_ratio": jump_ratio,
                        "min_recent_cluster_confirmations": confirmations,
                        "volatility_action": vol_action,
                        "require_ask_confirmation": require_ask,
                        "max_ask_vs_sale_gap_percent": ask_gap,
                        "preset_name": "CUSTOM",
                        "destination_world_scope": destination,
                        "seller_world_ids": seller_ids,
                        "weekly_velocity_threshold": weekly_vel,
                        "same_dc_travel_minutes": same_dc_minutes,
                        "cross_dc_travel_minutes": cross_dc_minutes,
                        "reference_price_scope": ref_scope,
                        "sell_price_strategy": sell_strategy,
                        "min_markdown_pct": min_markdown,
                        "digest_format": "CARDS",
                        "digest_changed_only": digest_changed,
                        "digest_max_clean": max_clean,
                        "digest_max_review": max_review,
                        "digest_include_review": include_review,
                        "digest_include_universalis_links": include_universalis,
                        "digest_include_ultros_links": include_ultros,
                        "table_grouping_strategy": table_grouping,
                        "table_max_rows_per_item": table_max_rows,
                        "table_include_same_dc_best": table_same_dc,
                        "table_show_theoretical": table_theoretical,
                        "alert_grouping_strategy": alert_grouping,
                        "alert_max_rows_per_item": alert_max_rows,
                        "alert_include_same_dc_best": alert_same_dc,
                        "alert_show_theoretical": alert_theoretical,
                        "alert_profit_improvement_threshold_gil": improvement_gil,
                        "alert_profit_improvement_threshold_pct": improvement_pct,
                        "alert_frequency_mode": frequency_mode,
                        "alert_digest_interval_minutes": interval_minutes,
                        "alert_schedule_cron": if schedule_cron.trim().is_empty() { serde_json::Value::Null } else { serde_json::Value::String(schedule_cron) },
                        "alert_send_empty_digest": send_empty,
                        "alert_immediate_threshold_enabled": immediate_enabled,
                        "alert_immediate_min_net_profit": immediate_profit,
                        "alert_immediate_min_markdown_pct": immediate_markdown,
                        "alert_immediate_min_velocity": immediate_velocity,
                        "alert_immediate_max_per_hour": immediate_max_hour,
                    }),
                )
                .await;
                let _ = update_arbitrage_destination_endpoint_ids(pid, endpoint_ids).await;
                reload_profiles();
            });
        }
    };

    let toggle_arbitrage_endpoint = move |endpoint_id: i32| {
        set_selected_arbitrage_endpoint_ids.update(move |ids| {
            if let Some(index) = ids.iter().position(|id| *id == endpoint_id) {
                ids.remove(index);
            } else {
                ids.push(endpoint_id);
            }
        });
    };

    let profile_for_reset = profile.clone();
    let reset_alert_memory = move |_| {
        if let Some(p) = &profile_for_reset {
            let pid = p.id;
            spawn_local(async move {
                let _ = reset_arbitrage_delivery_state(pid).await;
            });
        }
    };

    let profile_for_preset = profile.clone();
    let apply_preset = std::rc::Rc::new(move |preset_name: &'static str| {
        if let Some(p) = &profile_for_preset {
            let pid = p.id;
            spawn_local(async move {
                if let Ok(settings) = apply_arbitrage_preset_api(pid, preset_name).await {
                    set_min_profit(settings.min_net_profit);
                    set_vel_thresh(settings.velocity_threshold);
                    set_travel_rate(settings.travel_cost_rate_per_min);
                    set_min_profit_t(settings.min_profit_total);
                    set_require_home_sell(settings.require_home_world_sell_target);
                    set_source_scope(settings.source_world_scope);
                    set_destination_scope(settings.destination_world_scope);
                    set_weekly_velocity_threshold(settings.weekly_velocity_threshold);
                    set_same_dc_travel_minutes(settings.same_dc_travel_minutes);
                    set_cross_dc_travel_minutes(settings.cross_dc_travel_minutes);
                    set_reference_price_scope(settings.reference_price_scope);
                    set_sell_price_strategy(settings.sell_price_strategy);
                    set_min_markdown_pct(settings.min_markdown_pct);
                    set_digest_max_clean(settings.digest_max_clean);
                    set_digest_max_review(settings.digest_max_review);
                    set_alert_digest_interval_minutes(settings.alert_digest_interval_minutes);
                    set_alert_immediate_min_net_profit(settings.alert_immediate_min_net_profit);
                    set_alert_immediate_min_markdown_pct(settings.alert_immediate_min_markdown_pct);
                    set_alert_immediate_min_velocity(settings.alert_immediate_min_velocity);
                    set_alert_immediate_max_per_hour(settings.alert_immediate_max_per_hour);
                }
            });
        }
    });
    let apply_conservative = {
        let apply_preset = apply_preset.clone();
        move |_| apply_preset("CONSERVATIVE")
    };
    let apply_balanced = {
        let apply_preset = apply_preset.clone();
        move |_| apply_preset("BALANCED")
    };
    let apply_aggressive = {
        let apply_preset = apply_preset.clone();
        move |_| apply_preset("AGGRESSIVE")
    };

    let profile_for_preview = profile.clone();
    let preview_digest = move |_| {
        if let Some(p) = &profile_for_preview {
            let pid = p.id;
            spawn_local(async move {
                if let Ok(preview) = preview_arbitrage_digest_api(pid).await {
                    set_arbitrage_preview(Some(preview));
                }
            });
        }
    };

    let profile_for_test = profile.clone();
    let send_test_digest = move |_| {
        if let Some(p) = &profile_for_test {
            let pid = p.id;
            spawn_local(async move {
                if let Ok(result) = test_arbitrage_digest_api(pid).await {
                    set_arbitrage_test_result(Some(result));
                }
                if let Ok(status) = get_arbitrage_alert_status_api(pid).await {
                    set_arbitrage_alert_status(Some(status));
                }
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
                    <div class="rounded-xl border border-white/10 bg-zinc-950/30 p-4 space-y-4">
                        <div>
                            <SettingHelpLabel
                                label="Recommended Presets"
                                tooltip="Applies a complete recommended configuration. Any later manual edit marks the profile as CUSTOM."
                            />
                            <div class="flex flex-wrap gap-2">
                                <button class="rounded-lg border border-white/10 px-3 py-2 text-xs font-semibold text-gray-300 hover:border-emerald-400/40 hover:bg-emerald-400/10" on:click=apply_conservative>"Conservative"</button>
                                <button class="rounded-lg border border-white/10 px-3 py-2 text-xs font-semibold text-gray-300 hover:border-violet-400/40 hover:bg-violet-400/10" on:click=apply_balanced>"Balanced"</button>
                                <button class="rounded-lg border border-white/10 px-3 py-2 text-xs font-semibold text-gray-300 hover:border-amber-400/40 hover:bg-amber-400/10" on:click=apply_aggressive>"Aggressive"</button>
                            </div>
                        </div>

                        <div class="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs text-gray-400">
                            {move || arbitrage_alert_status().map(|status| view! {
                                <div class="rounded-lg border border-white/10 bg-black/20 p-3">
                                    <div class="font-semibold text-gray-300">"Pending Digest"</div>
                                    <div>{format!("{} queued rows", status.pending_digest_count)}</div>
                                </div>
                                <div class="rounded-lg border border-white/10 bg-black/20 p-3">
                                    <div class="font-semibold text-gray-300">"Immediate Window"</div>
                                    <div>{format!("{} sent", status.immediate_sent_count)}</div>
                                </div>
                                <div class="rounded-lg border border-white/10 bg-black/20 p-3">
                                    <div class="font-semibold text-gray-300">"Next Digest"</div>
                                    <div>{status.next_digest_hint.unwrap_or_else(|| "n/a".to_string())}</div>
                                </div>
                            })}
                        </div>

                        <div class="flex flex-wrap gap-2">
                            <button class="rounded-lg border border-white/10 px-3 py-2 text-xs font-semibold text-gray-300 hover:border-sky-400/40 hover:bg-sky-400/10" on:click=preview_digest>"Preview Digest"</button>
                            <button class="rounded-lg border border-white/10 px-3 py-2 text-xs font-semibold text-gray-300 hover:border-emerald-400/40 hover:bg-emerald-400/10" on:click=send_test_digest>"Send Test"</button>
                        </div>

                        {move || arbitrage_test_result().map(|result| view! {
                            <div class=if result.delivered { "rounded-lg border border-emerald-400/20 bg-emerald-400/10 p-3 text-xs text-emerald-200" } else { "rounded-lg border border-rose-400/20 bg-rose-400/10 p-3 text-xs text-rose-200" }>
                                {format!("Test attempted {} endpoint(s), {} failed.", result.attempted, result.failed)}
                            </div>
                        })}

                        {move || arbitrage_preview().map(|preview| view! {
                            <div class="rounded-lg border border-white/10 bg-black/20 p-3 text-xs text-gray-300 space-y-2">
                                <div class="font-semibold text-gray-200">{preview.title}</div>
                                <div>{preview.body}</div>
                                <div class="space-y-1">
                                    {preview.embeds.into_iter().take(5).map(|embed| view! {
                                        <div class="rounded border border-white/10 px-2 py-1">
                                            <div class="font-semibold">{embed.title}</div>
                                            <div class="text-gray-500">{embed.description}</div>
                                        </div>
                                    }).collect::<Vec<_>>()}
                                </div>
                            </div>
                        })}
                    </div>

                    <div class="rounded-xl border border-white/10 bg-zinc-950/30 p-4">
                        <EndpointsPanel />
                    </div>

                    <div class="rounded-xl border border-white/10 bg-zinc-950/30 p-4 space-y-3">
                        <div class="flex items-start justify-between gap-3">
                            <div>
                                <SettingHelpLabel
                                    label="Arbitrage Alert Destinations"
                                    tooltip="Choose which existing alert endpoints receive arbitrage digests and immediate alerts. If none are selected, all endpoints are used as a fallback."
                                />
                                <p class="text-xs text-gray-500">"Create or edit endpoints above, then choose where arbitrage alerts should go."</p>
                            </div>
                            <button
                                class="rounded-lg border border-white/10 px-3 py-2 text-xs font-semibold text-gray-300 hover:border-amber-400/40 hover:bg-amber-400/10"
                                title="Clears changed-only digest memory, per-item profit memory, pending digest rows, and cadence counters."
                                on:click=reset_alert_memory
                            >
                                "Reset Alert Memory"
                            </button>
                        </div>
                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
                            {move || {
                                let selected = selected_arbitrage_endpoint_ids();
                                let endpoints = arbitrage_endpoints();
                                if endpoints.is_empty() {
                                    vec![view! {
                                        <div class="text-xs text-gray-500">"No endpoints available yet."</div>
                                    }.into_any()]
                                } else {
                                    endpoints.into_iter().map(|endpoint| {
                                        let id = endpoint.id;
                                        let checked = selected.contains(&id);
                                        let method_label = endpoint_method_label(&endpoint.method);
                                        view! {
                                            <label class="flex items-center gap-2 rounded-lg border border-white/10 bg-zinc-950/40 px-3 py-2 text-xs text-gray-300">
                                                <input
                                                    type="checkbox"
                                                    class="accent-violet-500"
                                                    prop:checked=checked
                                                    on:change=move |_| toggle_arbitrage_endpoint(id)
                                                />
                                                <span class="font-semibold">{endpoint.name}</span>
                                                <span class="text-gray-500">{format!("({method_label})")}</span>
                                            </label>
                                        }.into_any()
                                    }).collect::<Vec<_>>()
                                }
                            }}
                        </div>
                    </div>

                    <div class="grid grid-cols-2 gap-4">
                        <div>
                            <SettingHelpLabel
                                label="Min Net Profit (Gil)"
                                tooltip="Minimum profit after estimated travel cost is deducted. Higher values reduce noise but may hide smaller safe flips."
                            />
                            <input
                                type="number"
                                title="A flip is shown only when net profit is at least this amount."
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=min_profit
                                on:input=move |ev| set_min_profit(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                            />
                        </div>
                        <div>
                            <SettingHelpLabel
                                label="Velocity Threshold"
                                tooltip="Minimum current sales pressure: units sold in the last 48 hours divided by active destination listings. Higher values favor faster-moving items."
                            />
                            <input
                                type="number"
                                step="0.1"
                                title="Computed as recent destination units sold divided by active destination listings."
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=vel_thresh
                                on:input=move |ev| set_vel_thresh(event_target_value(&ev).parse::<f64>().unwrap_or(0.0))
                            />
                        </div>
                        <div>
                            <SettingHelpLabel
                                label="Travel Cost Rate (Gil/Min)"
                                tooltip="How much gil one minute of travel or setup time is worth to you. Used to reduce gross profit into net profit."
                            />
                            <input
                                type="number"
                                title="Example: if your time is worth 600,000 gil/hour, use 10,000 gil/min."
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=travel_rate
                                on:input=move |ev| set_travel_rate(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                            />
                        </div>
                        <div>
                            <SettingHelpLabel
                                label="Min Gross Profit (Gil)"
                                tooltip="Minimum gross profit before travel cost. This filters tiny spreads early."
                            />
                            <input
                                type="number"
                                title="Gross profit is (sell price - buy price) times quantity before travel cost."
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=min_profit_t
                                on:input=move |ev| set_min_profit_t(event_target_value(&ev).parse::<i64>().unwrap_or(0))
                            />
                        </div>
                        <div>
                            <SettingHelpLabel
                                label="Source Scope"
                                tooltip="Controls how far the scanner looks for cheap buy-side listings. Wider scopes can find more profit but increase travel friction."
                            />
                            <select
                                title="Same data center is the practical default; same region allows cross-DC buys; home world only is lowest friction."
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=source_scope
                                on:change=move |ev| set_source_scope(event_target_value(&ev))
                            >
                                <option value="SAME_DC">"Same data center"</option>
                                <option value="SAME_REGION">"Same region"</option>
                                <option value="CURRENT_WORLD">"Home world only"</option>
                            </select>
                        </div>
                        <div>
                            <SettingHelpLabel
                                label="Destination Scope"
                                tooltip="Controls which worlds can be considered sell-side destinations when home-world-only selling is disabled."
                            />
                            <select
                                title="Home world is the executable default. Active data center and same region can be useful for theoretical planning or multi-character selling."
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=destination_scope
                                on:change=move |ev| set_destination_scope(event_target_value(&ev))
                            >
                                <option value="HOME_WORLD">"Home world"</option>
                                <option value="ACTIVE_DC">"Active data center"</option>
                                <option value="SAME_REGION">"Same region"</option>
                                <option value="CUSTOM">"Custom seller worlds"</option>
                            </select>
                        </div>
                        <div>
                            <SettingHelpLabel
                                label="Custom Seller World IDs"
                                tooltip="Comma-separated world IDs where you can actually sell via alts or retainers. Used only when destination scope is Custom."
                            />
                            <input
                                type="text"
                                title="Example: 21, 22, 23"
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=seller_world_ids_text
                                on:input=move |ev| set_seller_world_ids_text(event_target_value(&ev))
                            />
                        </div>
                        <div>
                            <SettingHelpLabel
                                label="Weekly Velocity Floor"
                                tooltip="Minimum average units sold per day over the last 7 days. This is separate from the current velocity threshold."
                            />
                            <input
                                type="number"
                                step="0.1"
                                title="Calculated as total sales in the last 7 days divided by 7."
                                class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                prop:value=weekly_velocity_threshold
                                on:input=move |ev| set_weekly_velocity_threshold(event_target_value(&ev).parse::<f64>().unwrap_or(0.0))
                            />
                        </div>
                        <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                            <input
                                type="checkbox"
                                title="When enabled, sell-side destination is locked to your profile home world because FFXIV blocks market selling while visiting."
                                class="mt-1 accent-violet-500"
                                prop:checked=require_home_sell
                                on:change=move |ev| set_require_home_sell(event_target_checked(&ev))
                            />
                            <span>
                                <SettingHelpLabel
                                    label="Sell only on home world"
                                    tooltip="When enabled, sell-side destination is locked to your profile home world because FFXIV blocks market selling while visiting."
                                />
                                <span class="block text-xs text-gray-500">"Keeps arbitrage executable with FFXIV travel restrictions."</span>
                            </span>
                        </label>
                    </div>

                    <div class="rounded-xl border border-white/10 bg-zinc-950/30 p-4 space-y-4">
                        <h4 class="text-sm font-semibold text-gray-300">"Execution & Pricing"</h4>
                        <div class="grid grid-cols-2 gap-4">
                            <div>
                                <SettingHelpLabel
                                    label="Same-DC Travel Minutes"
                                    tooltip="Travel-time estimate for buying from another world in the same data center. This feeds net profit."
                                />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=same_dc_travel_minutes
                                    on:input=move |ev| set_same_dc_travel_minutes(event_target_value(&ev).parse::<i32>().unwrap_or(2))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel
                                    label="Cross-DC Travel Minutes"
                                    tooltip="Travel-time estimate for buying from another data center and returning to sell. This feeds net profit."
                                />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=cross_dc_travel_minutes
                                    on:input=move |ev| set_cross_dc_travel_minutes(event_target_value(&ev).parse::<i32>().unwrap_or(8))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel
                                    label="Reference Price Scope"
                                    tooltip="World set used for reference min/average price context in the table and alert text."
                                />
                                <select class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=reference_price_scope
                                    on:change=move |ev| set_reference_price_scope(event_target_value(&ev))
                                >
                                    <option value="DESTINATION_WORLD">"Destination world"</option>
                                    <option value="DESTINATION_DC">"Destination data center"</option>
                                    <option value="ACTIVE_REGION">"Active region"</option>
                                    <option value="SOURCE_AND_DESTINATION">"Source + destination"</option>
                                </select>
                            </div>
                            <div>
                                <SettingHelpLabel
                                    label="Sell Price Strategy"
                                    tooltip="Controls the price used as the expected sell reference for profit calculations."
                                />
                                <select class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=sell_price_strategy
                                    on:change=move |ev| set_sell_price_strategy(event_target_value(&ev))
                                >
                                    <option value="LOWER_OF_ASK_AND_MEDIAN">"Lower of ask and median"</option>
                                    <option value="DESTINATION_LOW_ASK">"Destination low ask"</option>
                                    <option value="MEDIAN_SALE">"Median sale"</option>
                                </select>
                            </div>
                            <div>
                                <SettingHelpLabel
                                    label="Min Markdown (%)"
                                    tooltip="Minimum percentage below the selected sell reference. Higher values favor deeper discounts."
                                />
                                <input type="number" step="0.1" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=min_markdown_pct
                                    on:input=move |ev| set_min_markdown_pct(event_target_value(&ev).parse::<f64>().unwrap_or(0.0))
                                />
                            </div>
                        </div>
                    </div>

                    <div class="rounded-xl border border-white/10 bg-zinc-950/30 p-4 space-y-4">
                        <h4 class="text-sm font-semibold text-gray-300">"Table Behavior"</h4>
                        <div class="grid grid-cols-2 gap-4">
                            <div>
                                <SettingHelpLabel label="Table Grouping" tooltip="Controls how many options per item are shown in the table." />
                                <select class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=table_grouping_strategy
                                    on:change=move |ev| set_table_grouping_strategy(event_target_value(&ev))
                                >
                                    <option value="BEST_PLUS_SAME_DC">"Best + same-DC fallback"</option>
                                    <option value="BEST_ONLY">"Best only"</option>
                                    <option value="ALL">"All rows"</option>
                                </select>
                            </div>
                            <div>
                                <SettingHelpLabel label="Table Rows / Item" tooltip="Maximum rows per item/HQ in the table after grouping." />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=table_max_rows_per_item
                                    on:input=move |ev| set_table_max_rows_per_item(event_target_value(&ev).parse::<i32>().unwrap_or(2))
                                />
                            </div>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=table_include_same_dc_best on:change=move |ev| set_table_include_same_dc_best(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Include same-DC fallback" tooltip="When the best row is cross-DC, also include the best same-DC row for the same item/HQ." /></span>
                            </label>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=table_show_theoretical on:change=move |ev| set_table_show_theoretical(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Show theoretical rows" tooltip="Shows rows whose sell destination is not directly executable with the current home-world selling setup." /></span>
                            </label>
                        </div>
                    </div>

                    <div class="rounded-xl border border-white/10 bg-zinc-950/30 p-4 space-y-4">
                        <h4 class="text-sm font-semibold text-gray-300">"Alert & Digest Behavior"</h4>
                        <div class="grid grid-cols-2 gap-4">
                            <div>
                                <SettingHelpLabel label="Alert Grouping" tooltip="Controls how many options per item are eligible for Discord delivery. This is separate from the table." />
                                <select class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_grouping_strategy
                                    on:change=move |ev| set_alert_grouping_strategy(event_target_value(&ev))
                                >
                                    <option value="BEST_PLUS_SAME_DC">"Best + same-DC fallback"</option>
                                    <option value="BEST_ONLY">"Best only"</option>
                                    <option value="ALL">"All rows"</option>
                                </select>
                            </div>
                            <div>
                                <SettingHelpLabel label="Alert Rows / Item" tooltip="Maximum rows per item/HQ that can be included in alert messages." />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_max_rows_per_item
                                    on:input=move |ev| set_alert_max_rows_per_item(event_target_value(&ev).parse::<i32>().unwrap_or(2))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel label="Profit Improvement (Gil)" tooltip="Same item/HQ alerts only send again when profit beats the previous delivered best by at least this much." />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_profit_improvement_threshold_gil
                                    on:input=move |ev| set_alert_profit_improvement_threshold_gil(event_target_value(&ev).parse::<i64>().unwrap_or(1))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel label="Profit Improvement (%)" tooltip="Optional percentage improvement required before the same item/HQ alerts again." />
                                <input type="number" step="0.1" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_profit_improvement_threshold_pct
                                    on:input=move |ev| set_alert_profit_improvement_threshold_pct(event_target_value(&ev).parse::<f64>().unwrap_or(0.0))
                                />
                            </div>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=alert_include_same_dc_best on:change=move |ev| set_alert_include_same_dc_best(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Alert same-DC fallback" tooltip="When the best alert candidate is cross-DC, include the best same-DC candidate too." /></span>
                            </label>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=alert_show_theoretical on:change=move |ev| set_alert_show_theoretical(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Alert theoretical rows" tooltip="Allows alert messages to include rows that may require alternate selling setup." /></span>
                            </label>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=digest_changed_only on:change=move |ev| set_digest_changed_only(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Changed-only digest" tooltip="Suppresses unchanged rows until ask prices, sale summaries, or tracked risk metrics change." /></span>
                            </label>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=digest_include_review on:change=move |ev| set_digest_include_review(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Include review section" tooltip="Includes volatile or regime-change rows in a separate review section." /></span>
                            </label>
                            <div>
                                <SettingHelpLabel label="Max Clean Rows" tooltip="Maximum clean opportunities included per digest message." />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=digest_max_clean
                                    on:input=move |ev| set_digest_max_clean(event_target_value(&ev).parse::<i32>().unwrap_or(8))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel label="Max Review Rows" tooltip="Maximum volatile/review opportunities included per digest message." />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=digest_max_review
                                    on:input=move |ev| set_digest_max_review(event_target_value(&ev).parse::<i32>().unwrap_or(4))
                                />
                            </div>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=digest_include_universalis_links on:change=move |ev| set_digest_include_universalis_links(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Universalis links" tooltip="Includes Universalis item links in digest text." /></span>
                            </label>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=digest_include_ultros_links on:change=move |ev| set_digest_include_ultros_links(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Ultros links" tooltip="Includes local market item links in digest text when available." /></span>
                            </label>
                        </div>
                    </div>

                    <div class="rounded-xl border border-white/10 bg-zinc-950/30 p-4 space-y-4">
                        <h4 class="text-sm font-semibold text-gray-300">"Cadence & Immediate Alerts"</h4>
                        <div class="grid grid-cols-2 gap-4">
                            <div>
                                <SettingHelpLabel label="Alert Frequency" tooltip="Controls normal digest cadence. Immediate threshold alerts can still fire independently." />
                                <select class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_frequency_mode
                                    on:change=move |ev| set_alert_frequency_mode(event_target_value(&ev))
                                >
                                    <option value="IMMEDIATE">"Immediate only"</option>
                                    <option value="DIGEST_INTERVAL">"Every X minutes"</option>
                                    <option value="SCANNER_COMPLETE">"After every scan"</option>
                                    <option value="SCHEDULED">"Scheduled"</option>
                                </select>
                            </div>
                            <div>
                                <SettingHelpLabel label="Digest Interval Minutes" tooltip="Minimum minutes between normal digest sends for interval or scheduled fallback modes." />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_digest_interval_minutes
                                    on:input=move |ev| set_alert_digest_interval_minutes(event_target_value(&ev).parse::<i32>().unwrap_or(60))
                                />
                            </div>
                            <div class="col-span-2">
                                <SettingHelpLabel label="Schedule Cron" tooltip="Optional 5- or 6-field cron expression for scheduled digest mode." />
                                <input type="text" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_schedule_cron
                                    on:input=move |ev| set_alert_schedule_cron(event_target_value(&ev))
                                />
                            </div>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=alert_send_empty_digest on:change=move |ev| set_alert_send_empty_digest(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Send empty digest" tooltip="Allows a digest message even when no opportunities qualify. Usually leave off." /></span>
                            </label>
                            <label class="flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input type="checkbox" class="mt-1 accent-violet-500" prop:checked=alert_immediate_threshold_enabled on:change=move |ev| set_alert_immediate_threshold_enabled(event_target_checked(&ev)) />
                                <span><SettingHelpLabel label="Immediate threshold alerts" tooltip="Sends an immediate alert when an opportunity exceeds the configured high-value thresholds." /></span>
                            </label>
                            <div>
                                <SettingHelpLabel label="Immediate Min Profit" tooltip="Minimum net profit required for immediate alert delivery." />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_immediate_min_net_profit
                                    on:input=move |ev| set_alert_immediate_min_net_profit(event_target_value(&ev).parse::<i64>().unwrap_or(500000))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel label="Immediate Min Markdown (%)" tooltip="Optional markdown percentage required for immediate alerts." />
                                <input type="number" step="0.1" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_immediate_min_markdown_pct
                                    on:input=move |ev| set_alert_immediate_min_markdown_pct(event_target_value(&ev).parse::<f64>().unwrap_or(0.0))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel label="Immediate Min Velocity" tooltip="Optional current velocity required for immediate alerts." />
                                <input type="number" step="0.1" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_immediate_min_velocity
                                    on:input=move |ev| set_alert_immediate_min_velocity(event_target_value(&ev).parse::<f64>().unwrap_or(0.0))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel label="Immediate Max / Hour" tooltip="Maximum immediate alert messages sent per hour for this profile." />
                                <input type="number" class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=alert_immediate_max_per_hour
                                    on:input=move |ev| set_alert_immediate_max_per_hour(event_target_value(&ev).parse::<i32>().unwrap_or(3))
                                />
                            </div>
                        </div>
                    </div>

                    <div class="rounded-xl border border-white/10 bg-zinc-950/30 p-4 space-y-4">
                        <h4 class="text-sm font-semibold text-gray-300">"Volatility & Review Gates"</h4>
                        <div class="grid grid-cols-2 gap-4">
                            <div>
                                <SettingHelpLabel
                                    label="Max Price Jump Ratio"
                                    tooltip="Recent-vs-prior price jump ratio that flags a possible regime change. 1.30 means recent sales average is at least 30% above prior sales."
                                />
                                <input
                                    type="number"
                                    step="0.01"
                                    title="Lower values flag more items as volatile; higher values allow larger recent price jumps."
                                    class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=max_price_jump_ratio
                                    on:input=move |ev| set_max_price_jump_ratio(event_target_value(&ev).parse::<f64>().unwrap_or(1.30))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel
                                    label="Recent Confirmations"
                                    tooltip="Minimum recent sales needed before a price jump can be called a confirmed regime change instead of an unconfirmed spike."
                                />
                                <input
                                    type="number"
                                    title="Higher values keep more jumps in review until more sales confirm the new price level."
                                    class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=min_recent_cluster_confirmations
                                    on:input=move |ev| set_min_recent_cluster_confirmations(event_target_value(&ev).parse::<i32>().unwrap_or(5))
                                />
                            </div>
                            <div>
                                <SettingHelpLabel
                                    label="Volatility Action"
                                    tooltip="What to do when an item has a suspicious recent price jump."
                                />
                                <select
                                    title="Suppress hides volatile rows; Review keeps them separate; Warn keeps them in the main table with a warning."
                                    class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=volatility_action
                                    on:change=move |ev| set_volatility_action(event_target_value(&ev))
                                >
                                    <option value="DEMOTE_TO_REVIEW">"Demote to review"</option>
                                    <option value="SUPPRESS">"Suppress"</option>
                                    <option value="ALERT_WITH_WARNING">"Warn only"</option>
                                </select>
                            </div>
                            <div>
                                <SettingHelpLabel
                                    label="Max Ask Gap (%)"
                                    tooltip="Maximum allowed difference between current low asks and the recent sale cluster. If asks disagree too much, a confirmed jump is downgraded to unconfirmed."
                                />
                                <input
                                    type="number"
                                    step="0.1"
                                    title="Lower values require live listings to closely corroborate recent sales; higher values tolerate noisier ask prices."
                                    class="p-2.5 rounded-lg bg-zinc-950/80 border border-white/10 text-sm focus:outline-none focus:border-violet-500/50 w-full text-gray-200"
                                    prop:value=max_ask_vs_sale_gap_percent
                                    on:input=move |ev| set_max_ask_vs_sale_gap_percent(event_target_value(&ev).parse::<f64>().unwrap_or(15.0))
                                />
                            </div>
                            <label class="col-span-2 flex items-start gap-3 rounded-xl border border-white/10 bg-zinc-950/30 p-3">
                                <input
                                    type="checkbox"
                                    title="When enabled, current low asks must support the recent sale cluster before a price jump is considered confirmed."
                                    class="mt-1 accent-violet-500"
                                    prop:checked=require_ask_confirmation
                                    on:change=move |ev| set_require_ask_confirmation(event_target_checked(&ev))
                                />
                                <span>
                                    <SettingHelpLabel
                                        label="Require ask confirmation"
                                        tooltip="When enabled, current low asks must support the recent sale cluster before a price jump is considered confirmed."
                                    />
                                    <span class="block text-xs text-gray-500">"Downgrades jumps when live listings do not corroborate recent sale prices."</span>
                                </span>
                            </label>
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
