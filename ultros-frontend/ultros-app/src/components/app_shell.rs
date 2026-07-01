use crate::components::ad::DesktopAdRail;
use crate::components::side_nav::SideNav;
use crate::components::top_bar::TopBar;
use crate::global_state::side_nav::provide_side_nav_settings;
use leptos::prelude::*;
use leptos_router::hooks::use_location;

/// Application shell: persistent sidebar + slim topbar + fluid content
/// + optional ad rail. Mobile collapses the sidebar into a hamburger-
/// toggled overlay drawer.
#[component]
pub fn AppShell(children: Children) -> impl IntoView {
    let nav = provide_side_nav_settings();
    let location = use_location();
    let is_not_found = use_context::<RwSignal<bool>>().unwrap_or_else(|| RwSignal::new(false));

    // Dismiss the mobile drawer on any navigation.
    Effect::new(move |_| {
        let _ = location.pathname.get();
        nav.drawer_open.set(false);
        is_not_found.set(false);
    });

    // Escape closes the drawer.
    let on_keydown = move |ev: leptos::ev::KeyboardEvent| {
        if ev.key() == "Escape" && nav.drawer_open.get_untracked() {
            nav.drawer_open.set(false);
        }
    };

    let drawer_open = nav.drawer_open;
    let collapsed = nav.collapsed;

    let shell_classes = move || {
        let mut classes = String::from("app-shell");
        if collapsed.get() {
            classes.push_str(" app-shell-collapsed");
        }
        if drawer_open.get() {
            classes.push_str(" app-shell-drawer-open");
        }
        classes
    };

    let children_view = children();
    let is_not_found_classes = move || {
        if is_not_found.get() {
            "min-h-screen w-full bg-zinc-950 flex items-center justify-center p-4"
        } else {
            "app-shell-content"
        }
    };
    let is_not_found_style = move || {
        if is_not_found.get() {
            "background-color: var(--color-background);"
        } else {
            ""
        }
    };

    view! {
        <div class=shell_classes on:keydown=on_keydown>
            {move || (!is_not_found.get()).then(|| view! {
                <SideNav />

                <div
                    class="app-shell-backdrop"
                    aria-hidden="true"
                    on:click=move |_| drawer_open.set(false)
                />

                <TopBar />
            })}

            <main class=is_not_found_classes style=is_not_found_style role="main">
                {children_view}
            </main>

            {move || (!is_not_found.get()).then(|| view! {
                <div class="app-shell-ad-rail">
                    <DesktopAdRail />
                </div>
            })}
        </div>
    }
}
