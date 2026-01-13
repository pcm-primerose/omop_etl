def make_validated_property(name: str, validator):
    """Factory for creating validated properties for questionnaire fields."""
    private_attr = f"_{name}"

    def getter(self):
        return getattr(self, private_attr, None)

    def setter(self, value):
        validated = validator(value=value, field_name=name)
        setattr(self, private_attr, validated)
        self.updated_fields.add(name)

    return property(getter, setter)
