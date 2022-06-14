use proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(Insertable, attributes(insertable))]
pub fn insertable_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_insertable(&ast)
}

fn impl_insertable(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;

    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    let fields = get_struct_fields(&ast);
    let attr = get_insertable_attribute(&ast);
    let InsertableAttr { db, table_name } = attr.parse_args().unwrap();

    let gen = quote! {
        impl #impl_generics sqlx_plus::Insertable for #name #ty_generics #where_clause {
            type Database = #db;

            fn table_name() -> &'static str {
                #table_name
            }

            fn insert_columns() -> Vec<&'static str> {
                vec![ #( stringify!(#fields) ),* ]
            }

            fn bind_fields<'q, Q>(&'q self, q: Q) -> Q
            where
                Q: QueryBindExt<'q, Self::Database>
            {
                q #( .bind(&self.#fields) )*
            }
        }
    };

    gen.into()
}

fn get_struct_fields(ast: &syn::DeriveInput) -> Vec<syn::Ident> {
    match ast.data {
        syn::Data::Struct(ref data_struct) => match data_struct.fields {
            syn::Fields::Named(ref fields_named) => fields_named
                .named
                .iter()
                .map(|field| field.ident.clone().unwrap())
                .collect::<Vec<_>>(),
            syn::Fields::Unnamed(_) => panic!("Can not tuple structs derive Insertable trait"),
            syn::Fields::Unit => panic!("Can not unit structs derive Insertable trait"),
        },
        _ => panic!("Only structs can derive Insertable trait"),
    }
}

fn get_insertable_attribute(ast: &syn::DeriveInput) -> &syn::Attribute {
    ast.attrs
        .iter()
        .filter(|x| x.path.is_ident("insertable"))
        .next()
        .expect("The insertable attribute is required for specifying DB type and table name")
}

struct InsertableAttr {
    db: syn::Path,
    table_name: String,
}

impl syn::parse::Parse for InsertableAttr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let db: syn::Path = input.parse()?;
        input.parse::<syn::Token![,]>()?;
        let table: syn::LitStr = input.parse()?;

        Ok(InsertableAttr {
            db,
            table_name: table.value(),
        })
    }
}
