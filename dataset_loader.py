def flatten_datasets(raw: dict[str, Any],
                     base_prefix: str | None = None,
                     defaults: dict[str, Any] | None = None,
                     prefix_parts: list[str] | None = None) -> dict[str, DatasetSpec]:
    """
    Recursively walk dataset groups, inheriting _base and _layout, returning
    {'intraday.trades': DatasetSpec, ...}
    """
    prefix_parts = prefix_parts or []
    acc: dict[str, DatasetSpec] = {}
    defaults = defaults or {}
    base_prefix = base_prefix or defaults.get("_base", "")

    for key, node in raw.items():
        if key.startswith("_"):
            continue  # skip meta
        if isinstance(node, dict) and "partitioning" not in node:
            # ↳ subgroup
            next_defaults = {**defaults, **{k: v for k, v in node.items() if k.startswith("_")}}
            acc.update(flatten_datasets(node,
                                        base_prefix=next_defaults.get("_base", base_prefix),
                                        defaults=next_defaults,
                                        prefix_parts=prefix_parts + [key]))
        else:
            fq_name = ".".join(prefix_parts + [key])
            spec_cfg = {**defaults, **node}
            spec_cfg.pop("_base", None)
            spec_cfg.pop("_layout", None)
            acc[fq_name] = DatasetSpec.from_dict(
                fq_name,
                {
                    "base_path": base_prefix,
                    "layout": spec_cfg.pop("layout", defaults.get("_layout", "{date}")),
                    **spec_cfg
                }
            )
    return acc
