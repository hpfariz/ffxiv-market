use leptos::prelude::*;

#[component]
pub fn NotFound() -> impl IntoView {
    // Notify context that we are in NotFound state
    let is_not_found = use_context::<RwSignal<bool>>();
    Effect::new(move |_| {
        if let Some(signal) = is_not_found {
            signal.set(true);
        }
    });

    view! {
        <div class="text-center space-y-2 select-none">
            <h1 class="text-6xl font-extrabold text-zinc-600 tracking-wider">"404"</h1>
            <p class="text-lg text-zinc-500 font-medium tracking-wide uppercase">"Not Found"</p>
        </div>
    }
}
