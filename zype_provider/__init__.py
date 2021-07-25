## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features. We recognize it's a bit unclean to define these in multiple places, but at this point it's the only workaround if you'd like your custom conn type to show up in the Airflow UI.
def get_provider_info():
    return {
        "package-name": "airflow-provider-zype",  # Required
        "name": "Zype Airflow Provider",  # Required
        "description": "A zype template for airflow providers.",  # Required
        "hook-class-names": ["zype_provider.hooks.zype_hook.ZypeHook"],
        "extra-links": ["zype_provider.operators.zype_operator.ExtraLink"],
        "versions": ["0.0.1"],  # Required
    }
