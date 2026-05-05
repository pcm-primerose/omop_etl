def setter_name(prop: property) -> str:
    """
    Return the name of a property's setter, assumes the property has one.
    """
    fset = prop.fset
    assert fset is not None, "property has no setter"
    return getattr(fset, "__name__")


class TrackedValidated:
    """
    Set and validate scalars with StrictValidators.
    """

    updated_fields: set[str]

    def _set_validated_prop(
        self,
        prop: property,
        value,
        validator,
        **validator_kwargs,
    ) -> None:
        name = setter_name(prop)
        private_attr = f"_{name}"
        setattr(self, private_attr, validator(value=value, field_name=name, **validator_kwargs))
        self.updated_fields.add(name)
