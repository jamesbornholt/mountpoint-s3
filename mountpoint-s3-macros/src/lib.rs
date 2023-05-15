use proc_macro2::TokenStream;
use syn::{Result, Signature};
use quote::{ToTokens, quote, quote_spanned};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{ItemFn, Token, Ident, parenthesized};

#[derive(Debug)]
struct Argument {
    name: Ident,
    value: Option<Ident>,
}

impl Parse for Argument {
    fn parse(input: ParseStream) -> Result<Self> {
        // let name = Punctuated::parse_separated_nonempty(input)?;
        let name = input.parse::<Ident>()?;
        let value = if input.peek(Token![=]) {
            let _ = input.parse::<Token![=]>();
            Some(input.parse()?)
        } else {
            None
        };
        Ok(Self { name, value })
    }
}

#[derive(Debug, Default)]
struct Arguments {
    arguments: Punctuated<Argument, Token![,]>,
}

impl Parse for Arguments {
    fn parse(input: ParseStream) -> Result<Self> {
        let _ = input.parse::<kw::args>();
        let content;
        let _ = parenthesized!(content in input);
        let arguments = content.parse_terminated(Argument::parse, Token![,])?;
        Ok(Self { arguments })
    }
}

#[derive(Debug, Default)]
struct InstrumentRequest {
    arguments: Arguments,
}

impl Parse for InstrumentRequest {
    fn parse(input: ParseStream) -> Result<Self> {
        println!("input: {:?}", input);

        let mut request = Self::default();
        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(kw::args) {
                request.arguments = input.parse()?;
            } else {
                return Err(lookahead.error());
            }
        }

        println!("parsed: {:?}", request);

        Ok(request)
    }
}

fn gen_instrument(args: InstrumentRequest, item: proc_macro::TokenStream) -> Result<proc_macro2::TokenStream> {
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

    let span = quote!(tracing::span!(tracing::Level::DEBUG, #name, bar=5));

    let body = quote_spanned!{block.span() =>
        let __instrument_span = #span;
        let __instrument_guard = __instrument_span.enter();
        #block
    };

    let func = quote!(
        #(#attrs)*
        #vis #constness #unsafety #asyncness #abi fn #ident<#params>(#inputs) #output
        #where_clause
        {
            #body
        }
    );

    eprintln!("TOKENS: {}", func);

    Ok(func)
}

#[proc_macro_attribute]
pub fn instrument_request(attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let args = syn::parse_macro_input!(attr as InstrumentRequest);

    match gen_instrument(args, item) {
        Ok(item) => item.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

mod kw {
    syn::custom_keyword!(args);
}