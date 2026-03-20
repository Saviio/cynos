use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

use cynos_core::schema::{ForeignKey, Table};
use cynos_core::DataType;
use cynos_storage::TableCache;
use hashbrown::HashSet;

use crate::catalog::table_type_name;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphqlSchema {
    pub scalars: Vec<ScalarTypeDef>,
    pub enums: Vec<EnumTypeDef>,
    pub input_objects: Vec<InputObjectTypeDef>,
    pub objects: Vec<ObjectTypeDef>,
    pub query: ObjectTypeDef,
    pub mutation: Option<ObjectTypeDef>,
    pub subscription: Option<ObjectTypeDef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScalarTypeDef {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumTypeDef {
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputObjectTypeDef {
    pub name: String,
    pub fields: Vec<InputValueDef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectTypeDef {
    pub name: String,
    pub fields: Vec<FieldDef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldDef {
    pub name: String,
    pub args: Vec<InputValueDef>,
    pub ty: TypeRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputValueDef {
    pub name: String,
    pub ty: TypeRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeRef {
    Named { name: String, non_null: bool },
    List { inner: Box<TypeRef>, non_null: bool },
}

impl TypeRef {
    pub fn named(name: impl Into<String>, non_null: bool) -> Self {
        Self::Named {
            name: name.into(),
            non_null,
        }
    }

    pub fn list(inner: TypeRef, non_null: bool) -> Self {
        Self::List {
            inner: Box::new(inner),
            non_null,
        }
    }

    pub fn render(&self) -> String {
        match self {
            Self::Named { name, non_null } => {
                if *non_null {
                    format!("{}!", name)
                } else {
                    name.clone()
                }
            }
            Self::List { inner, non_null } => {
                if *non_null {
                    format!("[{}]!", inner.render())
                } else {
                    format!("[{}]", inner.render())
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ScalarFilterKind {
    Boolean,
    Int,
    Long,
    Float,
    String,
    DateTime,
    Bytes,
    Json,
}

impl GraphqlSchema {
    pub fn from_table_cache(cache: &TableCache) -> Self {
        let table_names = cache.table_names();
        let mut tables = Vec::with_capacity(table_names.len());
        let mut type_names = BTreeMap::new();

        for table_name in table_names {
            if let Some(store) = cache.get_table(table_name) {
                let table = store.schema();
                type_names.insert(table.name().to_string(), table_type_name(table.name()));
                tables.push(table.clone());
            }
        }

        let reverse_relations = build_reverse_relations(&tables);
        let mut scalar_names = HashSet::new();
        scalar_names.insert("Long".to_string());
        scalar_names.insert("DateTime".to_string());
        scalar_names.insert("Bytes".to_string());
        scalar_names.insert("JSON".to_string());

        let mut objects = Vec::with_capacity(tables.len());
        let mut input_objects = Vec::new();
        let mut enums = vec![EnumTypeDef {
            name: "SortDirection".to_string(),
            values: vec!["ASC".to_string(), "DESC".to_string()],
        }];
        let scalar_filter_defs = scalar_filter_definitions();

        for table in &tables {
            objects.push(build_object_type(table, &type_names, &reverse_relations));
            enums.push(build_order_enum(table, &type_names));
            input_objects.push(build_where_input(table));
            input_objects.push(build_order_input(table, &type_names));
            input_objects.push(build_insert_input(table, &type_names));
            input_objects.push(build_patch_input(table, &type_names));

            if let Some(pk_input) = build_pk_input(table, &type_names) {
                input_objects.push(pk_input);
            }
        }

        input_objects.extend(scalar_filter_defs);

        let query_fields = build_query_fields(&tables, &type_names);
        let mutation_fields = build_mutation_fields(&tables, &type_names);
        let subscription_fields = build_subscription_fields(&tables, &type_names);

        let mut scalars: Vec<ScalarTypeDef> = scalar_names
            .into_iter()
            .map(|name| ScalarTypeDef { name })
            .collect();
        scalars.sort_by(|left, right| left.name.cmp(&right.name));

        Self {
            scalars,
            enums,
            input_objects,
            objects,
            query: ObjectTypeDef {
                name: "Query".to_string(),
                fields: query_fields,
            },
            mutation: Some(ObjectTypeDef {
                name: "Mutation".to_string(),
                fields: mutation_fields,
            }),
            subscription: Some(ObjectTypeDef {
                name: "Subscription".to_string(),
                fields: subscription_fields,
            }),
        }
    }

    pub fn to_sdl(&self) -> String {
        let mut out = String::new();

        for scalar in &self.scalars {
            out.push_str("scalar ");
            out.push_str(&scalar.name);
            out.push_str("\n\n");
        }

        for enum_def in &self.enums {
            out.push_str("enum ");
            out.push_str(&enum_def.name);
            out.push_str(" {\n");
            for value in &enum_def.values {
                out.push_str("  ");
                out.push_str(value);
                out.push('\n');
            }
            out.push_str("}\n\n");
        }

        for input in &self.input_objects {
            out.push_str("input ");
            out.push_str(&input.name);
            out.push_str(" {\n");
            for field in &input.fields {
                out.push_str("  ");
                out.push_str(&field.name);
                out.push_str(": ");
                out.push_str(&field.ty.render());
                out.push('\n');
            }
            out.push_str("}\n\n");
        }

        for object in &self.objects {
            render_object(&mut out, object);
        }

        render_object(&mut out, &self.query);
        if let Some(mutation) = &self.mutation {
            render_object(&mut out, mutation);
        }
        if let Some(subscription) = &self.subscription {
            render_object(&mut out, subscription);
        }
        out
    }
}

pub fn render_schema_sdl(cache: &TableCache) -> String {
    GraphqlSchema::from_table_cache(cache).to_sdl()
}

fn render_object(out: &mut String, object: &ObjectTypeDef) {
    out.push_str("type ");
    out.push_str(&object.name);
    out.push_str(" {\n");
    for field in &object.fields {
        out.push_str("  ");
        out.push_str(&field.name);
        if !field.args.is_empty() {
            out.push('(');
            for (index, arg) in field.args.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&arg.name);
                out.push_str(": ");
                out.push_str(&arg.ty.render());
            }
            out.push(')');
        }
        out.push_str(": ");
        out.push_str(&field.ty.render());
        out.push('\n');
    }
    out.push_str("}\n\n");
}

fn build_object_type(
    table: &Table,
    type_names: &BTreeMap<String, String>,
    reverse_relations: &BTreeMap<String, Vec<ForeignKey>>,
) -> ObjectTypeDef {
    let mut fields = Vec::new();
    let mut used_names = HashSet::new();

    for column in table.columns() {
        let field_name = column.name().to_string();
        used_names.insert(field_name.clone());
        fields.push(FieldDef {
            name: field_name,
            args: Vec::new(),
            ty: graphql_type_for_column(column.data_type(), !column.is_nullable()),
        });
    }

    for fk in table.constraints().get_foreign_keys() {
        let parent_type = type_names
            .get(&fk.parent_table)
            .cloned()
            .unwrap_or_else(|| table_type_name(&fk.parent_table));
        let mut name = fk
            .graphql_forward_field()
            .map(ToString::to_string)
            .unwrap_or_else(|| fk.parent_table.clone());
        if used_names.contains(&name) {
            name = format!("{}_rel", name);
        }
        used_names.insert(name.clone());
        fields.push(FieldDef {
            name,
            args: Vec::new(),
            ty: TypeRef::named(parent_type, false),
        });
    }

    if let Some(reverse) = reverse_relations.get(table.name()) {
        for fk in reverse {
            let child_type = type_names
                .get(&fk.child_table)
                .cloned()
                .unwrap_or_else(|| table_type_name(&fk.child_table));
            let mut name = fk
                .graphql_reverse_field()
                .map(ToString::to_string)
                .unwrap_or_else(|| fk.child_table.clone());
            if used_names.contains(&name) {
                name = format!("{}_rel", name);
            }
            used_names.insert(name.clone());
            fields.push(FieldDef {
                name,
                args: collection_arguments(&child_type),
                ty: TypeRef::list(TypeRef::named(child_type, true), true),
            });
        }
    }

    ObjectTypeDef {
        name: type_names
            .get(table.name())
            .cloned()
            .unwrap_or_else(|| table_type_name(table.name())),
        fields,
    }
}

fn build_query_fields(tables: &[Table], type_names: &BTreeMap<String, String>) -> Vec<FieldDef> {
    let mut fields = Vec::new();
    for table in tables {
        let table_name = table.name().to_string();
        let type_name = type_names
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| table_type_name(&table_name));
        fields.push(FieldDef {
            name: table_name.clone(),
            args: collection_arguments(&type_name),
            ty: TypeRef::list(TypeRef::named(type_name.clone(), true), true),
        });

        if table.primary_key().is_some() {
            fields.push(FieldDef {
                name: format!("{}ByPk", table_name),
                args: vec![InputValueDef {
                    name: "pk".to_string(),
                    ty: TypeRef::named(format!("{}PkInput", type_name), true),
                }],
                ty: TypeRef::named(type_name, false),
            });
        }
    }
    fields
}

fn build_mutation_fields(
    tables: &[Table],
    type_names: &BTreeMap<String, String>,
) -> Vec<FieldDef> {
    let mut fields = Vec::new();
    for table in tables {
        let table_name = table.name().to_string();
        let type_name = type_names
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| table_type_name(&table_name));

        fields.push(FieldDef {
            name: format!("insert{}", type_name),
            args: vec![InputValueDef {
                name: "input".to_string(),
                ty: TypeRef::list(TypeRef::named(format!("{}InsertInput", type_name), true), true),
            }],
            ty: TypeRef::list(TypeRef::named(type_name.clone(), true), true),
        });

        let mut update_args = collection_arguments(&type_name);
        update_args.insert(
            0,
            InputValueDef {
                name: "set".to_string(),
                ty: TypeRef::named(format!("{}PatchInput", type_name), true),
            },
        );
        fields.push(FieldDef {
            name: format!("update{}", type_name),
            args: update_args,
            ty: TypeRef::list(TypeRef::named(type_name.clone(), true), true),
        });

        fields.push(FieldDef {
            name: format!("delete{}", type_name),
            args: collection_arguments(&type_name),
            ty: TypeRef::list(TypeRef::named(type_name, true), true),
        });
    }
    fields
}

fn build_subscription_fields(
    tables: &[Table],
    type_names: &BTreeMap<String, String>,
) -> Vec<FieldDef> {
    build_query_fields(tables, type_names)
}

fn collection_arguments(type_name: &str) -> Vec<InputValueDef> {
    vec![
        InputValueDef {
            name: "where".to_string(),
            ty: TypeRef::named(format!("{}WhereInput", type_name), false),
        },
        InputValueDef {
            name: "orderBy".to_string(),
            ty: TypeRef::list(TypeRef::named(format!("{}OrderByInput", type_name), true), false),
        },
        InputValueDef {
            name: "limit".to_string(),
            ty: TypeRef::named("Int", false),
        },
        InputValueDef {
            name: "offset".to_string(),
            ty: TypeRef::named("Int", false),
        },
    ]
}

fn build_where_input(table: &Table) -> InputObjectTypeDef {
    let type_name = table_type_name(table.name());
    let mut fields = vec![
        InputValueDef {
            name: "AND".to_string(),
            ty: TypeRef::list(TypeRef::named(format!("{}WhereInput", type_name), true), false),
        },
        InputValueDef {
            name: "OR".to_string(),
            ty: TypeRef::list(TypeRef::named(format!("{}WhereInput", type_name), true), false),
        },
    ];

    for column in table.columns() {
        let filter_name = scalar_filter_name(column.data_type());
        fields.push(InputValueDef {
            name: column.name().to_string(),
            ty: TypeRef::named(filter_name, false),
        });
    }

    InputObjectTypeDef {
        name: format!("{}WhereInput", type_name),
        fields,
    }
}

fn build_order_enum(table: &Table, type_names: &BTreeMap<String, String>) -> EnumTypeDef {
    EnumTypeDef {
        name: format!(
            "{}OrderField",
            type_names
                .get(table.name())
                .cloned()
                .unwrap_or_else(|| table_type_name(table.name()))
        ),
        values: table
            .columns()
            .iter()
            .map(|column| to_graphql_enum_value(column.name()))
            .collect(),
    }
}

fn build_order_input(table: &Table, type_names: &BTreeMap<String, String>) -> InputObjectTypeDef {
    let type_name = type_names
        .get(table.name())
        .cloned()
        .unwrap_or_else(|| table_type_name(table.name()));
    InputObjectTypeDef {
        name: format!("{}OrderByInput", type_name),
        fields: vec![
            InputValueDef {
                name: "field".to_string(),
                ty: TypeRef::named(format!("{}OrderField", type_name), true),
            },
            InputValueDef {
                name: "direction".to_string(),
                ty: TypeRef::named("SortDirection", true),
            },
        ],
    }
}

fn build_pk_input(
    table: &Table,
    type_names: &BTreeMap<String, String>,
) -> Option<InputObjectTypeDef> {
    let pk = table.primary_key()?;
    let mut fields = Vec::new();
    for column in pk.columns() {
        if let Some(table_column) = table.get_column(&column.name) {
            fields.push(InputValueDef {
                name: column.name.clone(),
                ty: graphql_type_for_column(table_column.data_type(), true),
            });
        }
    }
    Some(InputObjectTypeDef {
        name: format!(
            "{}PkInput",
            type_names
                .get(table.name())
                .cloned()
                .unwrap_or_else(|| table_type_name(table.name()))
        ),
        fields,
    })
}

fn build_insert_input(table: &Table, type_names: &BTreeMap<String, String>) -> InputObjectTypeDef {
    let fields = table
        .columns()
        .iter()
        .map(|column| InputValueDef {
            name: column.name().to_string(),
            ty: graphql_type_for_column(column.data_type(), !column.is_nullable()),
        })
        .collect();
    InputObjectTypeDef {
        name: format!(
            "{}InsertInput",
            type_names
                .get(table.name())
                .cloned()
                .unwrap_or_else(|| table_type_name(table.name()))
        ),
        fields,
    }
}

fn build_patch_input(table: &Table, type_names: &BTreeMap<String, String>) -> InputObjectTypeDef {
    let fields = table
        .columns()
        .iter()
        .map(|column| InputValueDef {
            name: column.name().to_string(),
            ty: graphql_type_for_column(column.data_type(), false),
        })
        .collect();
    InputObjectTypeDef {
        name: format!(
            "{}PatchInput",
            type_names
                .get(table.name())
                .cloned()
                .unwrap_or_else(|| table_type_name(table.name()))
        ),
        fields,
    }
}

fn build_reverse_relations(tables: &[Table]) -> BTreeMap<String, Vec<ForeignKey>> {
    let mut map: BTreeMap<String, Vec<ForeignKey>> = BTreeMap::new();
    for table in tables {
        for fk in table.constraints().get_foreign_keys() {
            map.entry(fk.parent_table.clone()).or_default().push(fk.clone());
        }
    }
    map
}

fn scalar_filter_definitions() -> Vec<InputObjectTypeDef> {
    vec![
        scalar_filter_definition("BooleanFilterInput", ScalarFilterKind::Boolean),
        scalar_filter_definition("IntFilterInput", ScalarFilterKind::Int),
        scalar_filter_definition("LongFilterInput", ScalarFilterKind::Long),
        scalar_filter_definition("FloatFilterInput", ScalarFilterKind::Float),
        scalar_filter_definition("StringFilterInput", ScalarFilterKind::String),
        scalar_filter_definition("DateTimeFilterInput", ScalarFilterKind::DateTime),
        scalar_filter_definition("BytesFilterInput", ScalarFilterKind::Bytes),
        scalar_filter_definition("JsonFilterInput", ScalarFilterKind::Json),
    ]
}

fn scalar_filter_definition(name: &str, kind: ScalarFilterKind) -> InputObjectTypeDef {
    let scalar_name = match kind {
        ScalarFilterKind::Boolean => "Boolean",
        ScalarFilterKind::Int => "Int",
        ScalarFilterKind::Long => "Long",
        ScalarFilterKind::Float => "Float",
        ScalarFilterKind::String => "String",
        ScalarFilterKind::DateTime => "DateTime",
        ScalarFilterKind::Bytes => "Bytes",
        ScalarFilterKind::Json => "JSON",
    };

    let mut fields = vec![InputValueDef {
        name: "isNull".to_string(),
        ty: TypeRef::named("Boolean", false),
    }];

    match kind {
        ScalarFilterKind::Boolean => {
            fields.push(InputValueDef {
                name: "eq".to_string(),
                ty: TypeRef::named(scalar_name, false),
            });
            fields.push(InputValueDef {
                name: "ne".to_string(),
                ty: TypeRef::named(scalar_name, false),
            });
        }
        ScalarFilterKind::Json => {
            fields.push(InputValueDef {
                name: "path".to_string(),
                ty: TypeRef::named("String", false),
            });
            fields.push(InputValueDef {
                name: "eq".to_string(),
                ty: TypeRef::named(scalar_name, false),
            });
            fields.push(InputValueDef {
                name: "contains".to_string(),
                ty: TypeRef::named(scalar_name, false),
            });
            fields.push(InputValueDef {
                name: "exists".to_string(),
                ty: TypeRef::named("Boolean", false),
            });
        }
        ScalarFilterKind::String | ScalarFilterKind::Bytes => {
            fields.extend(common_comparison_fields(scalar_name));
            fields.push(InputValueDef {
                name: "like".to_string(),
                ty: TypeRef::named("String", false),
            });
        }
        ScalarFilterKind::Int
        | ScalarFilterKind::Long
        | ScalarFilterKind::Float
        | ScalarFilterKind::DateTime => {
            fields.extend(common_comparison_fields(scalar_name));
            fields.push(InputValueDef {
                name: "between".to_string(),
                ty: TypeRef::list(TypeRef::named(scalar_name, true), false),
            });
        }
    }

    InputObjectTypeDef {
        name: name.to_string(),
        fields,
    }
}

fn common_comparison_fields(scalar_name: &str) -> Vec<InputValueDef> {
    vec![
        InputValueDef {
            name: "eq".to_string(),
            ty: TypeRef::named(scalar_name, false),
        },
        InputValueDef {
            name: "ne".to_string(),
            ty: TypeRef::named(scalar_name, false),
        },
        InputValueDef {
            name: "in".to_string(),
            ty: TypeRef::list(TypeRef::named(scalar_name, true), false),
        },
        InputValueDef {
            name: "notIn".to_string(),
            ty: TypeRef::list(TypeRef::named(scalar_name, true), false),
        },
        InputValueDef {
            name: "gt".to_string(),
            ty: TypeRef::named(scalar_name, false),
        },
        InputValueDef {
            name: "gte".to_string(),
            ty: TypeRef::named(scalar_name, false),
        },
        InputValueDef {
            name: "lt".to_string(),
            ty: TypeRef::named(scalar_name, false),
        },
        InputValueDef {
            name: "lte".to_string(),
            ty: TypeRef::named(scalar_name, false),
        },
    ]
}

fn graphql_type_for_column(data_type: DataType, non_null: bool) -> TypeRef {
    let type_name = match data_type {
        DataType::Boolean => "Boolean",
        DataType::Int32 => "Int",
        DataType::Int64 => "Long",
        DataType::Float64 => "Float",
        DataType::String => "String",
        DataType::DateTime => "DateTime",
        DataType::Bytes => "Bytes",
        DataType::Jsonb => "JSON",
    };
    TypeRef::named(type_name, non_null)
}

fn scalar_filter_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "BooleanFilterInput",
        DataType::Int32 => "IntFilterInput",
        DataType::Int64 => "LongFilterInput",
        DataType::Float64 => "FloatFilterInput",
        DataType::String => "StringFilterInput",
        DataType::DateTime => "DateTimeFilterInput",
        DataType::Bytes => "BytesFilterInput",
        DataType::Jsonb => "JsonFilterInput",
    }
}

fn to_graphql_enum_value(name: &str) -> String {
    let mut out = String::new();
    let mut previous_was_underscore = false;
    for ch in name.chars() {
        if ch == '_' {
            if !previous_was_underscore {
                out.push('_');
                previous_was_underscore = true;
            }
            continue;
        }
        previous_was_underscore = false;
        out.extend(ch.to_uppercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_core::DataType;

    fn build_cache() -> TableCache {
        let mut cache = TableCache::new();
        let users = TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();
        let orders = TableBuilder::new("orders")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("user_id", DataType::Int64)
            .unwrap()
            .add_column("total", DataType::Float64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_foreign_key("fk_orders_user", "user_id", "users", "id")
            .unwrap()
            .build()
            .unwrap();
        cache.create_table(users).unwrap();
        cache.create_table(orders).unwrap();
        cache
    }

    #[test]
    fn schema_includes_query_mutation_and_subscription_roots() {
        let cache = build_cache();
        let schema = GraphqlSchema::from_table_cache(&cache);
        let sdl = schema.to_sdl();
        assert!(sdl.contains("type Users"));
        assert!(sdl.contains("type Orders"));
        assert!(sdl.contains("type Query"));
        assert!(sdl.contains("type Mutation"));
        assert!(sdl.contains("type Subscription"));
        assert!(sdl.contains("users(where: UsersWhereInput"));
        assert!(sdl.contains("insertUsers(input: [UsersInsertInput!]!): [Users!]!"));
        assert!(sdl.contains("updateUsers(set: UsersPatchInput!"));
        assert!(sdl.contains("deleteUsers(where: UsersWhereInput"));
        assert!(sdl.contains("usersByPk(pk: UsersPkInput!): Users"));
    }

    #[test]
    fn schema_includes_relationship_and_mutation_inputs() {
        let cache = build_cache();
        let sdl = render_schema_sdl(&cache);
        assert!(sdl.contains("users(where: UsersWhereInput"));
        assert!(sdl.contains("[Orders!]!"));
        assert!(sdl.contains("users: Users"));
        assert!(sdl.contains("input UsersInsertInput"));
        assert!(sdl.contains("input UsersPatchInput"));
    }
}
