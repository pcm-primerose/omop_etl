class TrackedValidated:
    """
    Set and validate scalars with StrictValidators.
    """

    updated_fields: set[str]

    def _set_validated_prop(
        self,
        prop,  # property obj, e.g. self.__class__.icd10_code
        value,
        validator,
        **validator_kwargs,
    ) -> None:
        name = prop.fset.__name__
        private_attr = f"_{name}"
        setattr(self, private_attr, validator(value=value, field_name=name, **validator_kwargs))
        self.updated_fields.add(name)
