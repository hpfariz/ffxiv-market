use std::ops::Deref;

use crate::Cookies;
use crate::i18n::{t, t_string, use_i18n};
use leptos::{html::Ins, prelude::*};
use leptos_router::components::A;
use leptos_use::{UseMutationObserverOptions, use_mutation_observer_with_options};
use log::info;

#[component]
pub fn Ad(#[prop(optional)] class: Option<&'static str>) -> impl IntoView {
    let i18n = use_i18n();
    let ad_class = class.unwrap_or("h-64");
    let node = NodeRef::<Ins>::new();
    let cookies = use_context::<Cookies>().unwrap();
    let (hide_ads, _) = cookies.use_cookie_typed::<_, bool>("HIDE_ADS");
    let unfilled = RwSignal::new(false);
    let is_cleaned_up = StoredValue::new(false);
    on_cleanup(move || {
        is_cleaned_up.set_value(true);
    });

    let _mutation_observer = use_mutation_observer_with_options(
        node,
        move |mutations, _| {
            if is_cleaned_up.get_value() {
                return;
            }
            if let Some(_ad_fill_status) = mutations.into_iter().find(|record| {
                record
                    .attribute_name()
                    .map(|name| name == "data-ad-status")
                    .unwrap_or_default()
            }) {
                // just looking for data-ad-status="unfilled"
                if let Some(node) = node.get_untracked()
                    && let Some(status) = node.deref().get_attribute("data-ad-status")
                {
                    info!("ad status {status}");
                    let _ = unfilled.try_update(|val| {
                        *val = status == "unfilled";
                    });
                }
            }
        },
        UseMutationObserverOptions::default().attributes(true),
    );
    let ads_visible = Signal::derive(move || !hide_ads.get().unwrap_or_default());
    view! {
        <Show when=ads_visible>
            <div class:hidden=unfilled class="ad">
                <div class="flex flex-col h-full">
                    <span class="text-sm px-2 py-0.5 rounded-md border border-[color:var(--color-outline)] bg-[color:color-mix(in_srgb,_var(--brand-ring)_14%,_transparent)] text-[color:var(--color-text-muted)] shrink max-w-fit">
                        "Advertisements"
                    </span>
                    <script
                        async
                        src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-8789160460804755"
                        crossorigin="anonymous"
                        on:error=move |_e| {
                            if is_cleaned_up.get_value() {
                                return;
                            }
                            let _ = unfilled.try_update(|val| {
                                *val = true;
                            });
                        }
                    ></script>
                    // <!-- Ultros-Ad-Main -->
                    <ins
                        class=["adsbygoogle block ", ad_class].concat()

                        data-ad-client="ca-pub-8789160460804755"
                        data-ad-slot="1163555858"
                        // data-adtest="on"
                        node_ref=node
                    ></ins>
                    <script>(adsbygoogle = window.adsbygoogle || []).push({});</script>
                    <span class="text-neutral-500 italic text-sm">
                        "ads are optional. you may disable or enable them under "
                        <A href="/market/settings">{t!(i18n, ad_settings_link)}</A>
                    </span>
                </div>
            </div>
        </Show>
    }.into_any()
}

#[component]
pub fn DesktopAdRail() -> impl IntoView {
    let i18n = use_i18n();
    let cookies = use_context::<Cookies>().unwrap();
    let (hide_ads, _) = cookies.use_cookie_typed::<_, bool>("HIDE_ADS");
    let ads_visible = Signal::derive(move || !hide_ads.get().unwrap_or_default());

    view! {
        <Show when=ads_visible>
            <aside class="app-ad-rail" aria-label=t_string!(i18n, ad_aria_label)>
                <div class="ad-rail-slot sticky top-24">
                    <Ad class="h-[600px] w-full" />
                </div>
            </aside>
        </Show>
    }
}
