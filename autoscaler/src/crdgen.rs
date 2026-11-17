use kube::CustomResourceExt;
use kube_core::schema::{OptionalEnum, OptionalIntOrString};
use schemars::generate::SchemaSettings;
use schemars::transform::AddNullable;
use serde_json::{Map, Value};

// Reorder each properties map in the crd() schema to follow the struct field
// order in the schemars schema, recursing into nested objects
fn reorder(crd: &mut Value, schema: &Value) {
    if let (Some(props), Some(order)) = (
        crd.get_mut("properties").and_then(Value::as_object_mut),
        schema.get("properties").and_then(Value::as_object),
    ) {
        let mut ordered = Map::new();
        for (key, sub_schema) in order {
            if let Some(mut prop) = props.remove(key) {
                reorder(&mut prop, sub_schema);
                ordered.insert(key.clone(), prop);
            }
        }
        ordered.append(props);
        *props = ordered;
    }
    for key in ["items", "additionalProperties"] {
        if let (Some(c), Some(s)) = (crd.get_mut(key), schema.get(key)) {
            reorder(c, s);
        }
    }
}

fn main() {
    // crd() and kube-core's StructuralSchemaRewriter both sort schema properties,
    // so take only the field order from kube-derive 3.0.0's settings without it
    let generator = SchemaSettings::openapi3()
        .with(|s| {
            s.inline_subschemas = true;
            s.meta_schema = None;
        })
        .with_transform(AddNullable::default())
        .with_transform(OptionalEnum)
        .with_transform(OptionalIntOrString)
        .into_generator();
    let schema = serde_json::to_value(generator.into_root_schema_for::<capi_autoscaler::api::Autoscale>()).unwrap();

    let mut crd = serde_json::to_value(capi_autoscaler::api::Autoscale::crd()).unwrap();
    reorder(&mut crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"], &schema);
    print!("{}", serde_yaml::to_string(&crd).unwrap())
}
