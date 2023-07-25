use proc_macro2::TokenStream;
use syn::{Result, Signature, LitStr};
use quote::{quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{ItemFn, Token};

mod kw {
    syn::custom_keyword!(level);
    syn::custom_keyword!(expected_level);
}

#[derive(Debug, Default)]
struct LogFailuresArgs {
    level: Option<Level>,
    expected_level: Option<Level>,
}

impl Parse for LogFailuresArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut args = Self::default();
        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(kw::level) {
                let _ = input.parse::<kw::level>()?;
                args.level = Some(input.parse()?);
            } else if lookahead.peek(kw::expected_level) {
                let _ = input.parse::<kw::expected_level>()?;
                args.expected_level = Some(input.parse()?);
            } else {
                return Err(lookahead.error());
            }
            let lookahead = input.lookahead1();
            if lookahead.peek(Token![,]) {
                let _ = input.parse::<Token![,]>();
            } else if input.is_empty() {
                break;
            } else {
                return Err(lookahead.error());
            }
        }
        Ok(args)
    }
}

#[derive(Clone, Debug)]
enum Level {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl Level {
    fn to_tracing_event_macro(&self) -> TokenStream {
        match self {
            Level::Error => quote!(::tracing::error!),
            Level::Warn => quote!(::tracing::warn!),
            Level::Info => quote!(::tracing::info!),
            Level::Debug => quote!(::tracing::debug!),
            Level::Trace => quote!(::tracing::trace!),
        }
    }
}

impl Parse for Level {
    fn parse(input: ParseStream) -> Result<Self> {
        let _ = input.parse::<Token![=]>()?;
        let lookahead = input.lookahead1();
        if lookahead.peek(LitStr) {
            let str: LitStr = input.parse()?;
            match str.value() {
                s if s.eq_ignore_ascii_case("error") => Ok(Level::Error),
                s if s.eq_ignore_ascii_case("warn") => Ok(Level::Warn),
                s if s.eq_ignore_ascii_case("info") => Ok(Level::Info),
                s if s.eq_ignore_ascii_case("debug") => Ok(Level::Debug),
                s if s.eq_ignore_ascii_case("trace") => Ok(Level::Trace),
                _ => Err(input.error("unknown verbosity level")),
            }
        } else {
            Err(lookahead.error())
        }
    }
}

fn gen_log_failures(args: LogFailuresArgs, item: proc_macro::TokenStream) -> Result<TokenStream> {
    let func = syn::parse::<ItemFn>(item)?;

    let ItemFn {
        attrs,
        vis,
        sig,
        block
    } = func;

    let Signature {
        output,
        inputs,
        unsafety,
        asyncness,
        constness,
        abi,
        ident,
        generics: syn::Generics {
            params,
            where_clause,
            ..
        },
        ..
    } = sig;

    let name = ident.to_string();
    let name = quote!(#name);

    let return_type = match &output {
        syn::ReturnType::Default => quote!(()),
        syn::ReturnType::Type(_, typ) => quote!(#typ),
    };

    let LogFailuresArgs {
        level,
        expected_level,
    } = args;
    let level = level.unwrap_or(Level::Error).to_tracing_event_macro();
    let expected_level = expected_level.unwrap_or(Level::Error).to_tracing_event_macro();

    let match_body = quote_spanned!(block.span()=>
        Ok(x) => Ok(x),
        Err(e) => {
            let __expected = crate::fs::ExpectedError::expected_error(&e);
            #[allow(clippy::suspicious_else_formatting)]
            if __expected {
                #expected_level("{} failed: {e:#}", #name);
            } else {
                #level("{} failed: {e:#}", #name);
            }
            Err(e)
        }
    );

    let body = if asyncness.is_some() {
        quote_spanned!{block.span()=>
            let __result = async move { #block }.await;
            match __result {
                #match_body
            }
        }
    } else {
        quote_spanned!{block.span()=>
            #[allow(clippy::redundant_closure_call)]
            let __result: #return_type = (move || #block)();
            match __result {
                #match_body
            }
        }
    };

    let func = quote!(
        #(#attrs)*
        #vis #constness #unsafety #asyncness #abi fn #ident<#params>(#inputs) #output
        #where_clause
        {
            #body
        }
    );

    Ok(func)
}

/// A procedural macro that annotates a function with return type `Result<T, E>` and emits an event
/// to `tracing` whenever it returns `Err`.
///
/// This macro integrates with the [mountpoint_s3::ExpectedError] trait to allow controlling the
/// verbosity of "expected" failures separately from other failures. For example, calling `lookup`
/// on a file that doesn't exist is an "expected" failure, in the sense that it's the correct
/// behavior in response to an invalid user request.
///
/// The verbosity of the emitted events can be configured with the `level` and `expected_level`
/// keywords. For example:
///
/// ```ignore
/// #[log_failures(error="warn", expected_error="debug")]
/// fn foobar() -> Result<(), Error> {
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn log_failures(attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let args = syn::parse_macro_input!(attr as LogFailuresArgs);

    match gen_log_failures(args, item) {
        Ok(item) => item.into(),
        Err(err) => err.to_compile_error().into(),
    }
}