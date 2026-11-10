# jsonschema 2.x → 4.x 迁移说明

本次升级把 singer-python 对 jsonschema 的依赖从 `2.x` 迁到 `4.x`，
`$ref` 解析从已废弃的 `jsonschema.RefResolver` 迁移到 `referencing`
库，并显式固定为 **draft-4** 语义（`referencing.jsonschema.DRAFT4`），
不会因为库升级而自动切换到 2020-12。

注意：jsonschema 4.x 要求 Python >= 3.9（锁定的 4.26.0 要求 >= 3.10），
旧版 Python（如 3.5）无法使用升级后的版本。

## 真实受影响的调用

全仓搜索 jsonschema 相关用法，仅有以下调用点：

| 位置 | 变更 |
| --- | --- |
| `setup.py` `install_requires` | `jsonschema>=2.6.0,==2.*` → `jsonschema>=4.19.0,==4.*`，新增直接依赖 `referencing>=0.30.0,==0.*` |
| `requirements-domain.lock` | `jsonschema==2.6.0` → `jsonschema==4.26.0`，新增传递依赖锁 `attrs==26.1.0`、`jsonschema-specifications==2025.9.1`、`referencing==0.37.0`、`rpds-py==2026.6.3` |
| `singer/transform.py` 顶部 | `from jsonschema import RefResolver` → `referencing` 的 `Registry`/`Resource`/`DRAFT4` |
| `singer/transform.py` `resolve_schema_references` | `RefResolver("", schema, store=refs)` → 内部适配器 `_Draft4RefResolver` |
| `singer/transform.py` `_resolve_schema_references` | 增加递归引用检测（见下文差异 3） |

`Transformer` 本身（Decimal、boolean/integer、anyOf 等转换逻辑）不直接
调用 jsonschema，行为不变，由新增回归测试锁定。

## 行为前后差异（含样例）

### 1. 未注册的远端 ref：不再联网，立即明确失败

```python
resolve_schema_references({"properties": {"a": {"$ref": "http://example.com/x.json#/foo"}}})
```

- 2.x：`RefResolver` 对不在 store 里的 http(s) URI 会**真的发起网络下载**
  （`urlopen`），离线环境下表现为 `RefResolutionError: <urlopen error ...
  Temporary failure in name resolution>`，耗时不确定。
- 4.x：立即抛出 `referencing.exceptions.Unresolvable: No such resource:
  http://example.com/x.json#/foo`（实测 0.000s），不访问网络。

### 2. 未注册的本地相对 ref：异常类型变化

```python
resolve_schema_references({"properties": {"a": {"$ref": "missing.json#/foo"}}})
```

- 2.x：`jsonschema.RefResolutionError: unknown url type: 'missing.json'`
  （同样来自一次失败的抓取尝试）。
- 4.x：`referencing.exceptions.Unresolvable: No such resource: missing.json#/foo`。

下游若 `except jsonschema.RefResolutionError`，请改为
`except referencing.exceptions.Unresolvable`。jsonschema 4.x 仍保留
`jsonschema.exceptions.RefResolutionError` 作为 deprecated 别名，但访问它会
产生 `DeprecationWarning`，本库不再使用它。

### 3. 递归引用：确定性报错代替栈溢出

```python
schema = {"type": "object",
          "definitions": {"node": {"type": "object",
                                   "properties": {"child": {"$ref": "#/definitions/node"}}}},
          "properties": {"root": {"$ref": "#/definitions/node"}}}
resolve_schema_references(schema)
```

- 2.x：`RecursionError: maximum recursion depth exceeded`（内联展开无限递归）。
- 4.x：`ValueError: Cannot resolve recursive $ref: schema references itself
  and cannot be inlined`。递归引用本质上无法被内联展开，旧行为属于崩溃
 而非可依赖语义；新实现沿遍历栈检测环路并明确失败。

### 4. draft-4 `id` 锚点：旧实现不符合草案，现已修正

```python
schema = {"id": "root.json",
          "type": "object",
          "definitions": {"sub": {"id": "sub.json",
                                  "definitions": {"s": {"type": "string"}}}},
          "properties": {"a": {"$ref": "sub.json#/definitions/s"}}}
resolve_schema_references(schema)
```

- 2.x：`RefResolutionError: unknown url type: 'sub.json'`。旧代码始终用空
  base URI 解析，忽略了 draft-4 规定的 `id` 重基语义，把 `sub.json` 当成
  待抓取的 URL。
- 4.x：按 draft-4 正确解析，`properties.a` 得到 `{"type": "string"}`。
  这是旧行为本身不符合已声明草案（draft-4）的具体案例；若你的 schema
  依赖旧的失败行为（不太可能），升级后会变为解析成功。

### 5. 草案语义固定为 draft-4

`referencing` 资源统一用 `default_specification=DRAFT4` 构造：draft-4 的
`id` 是标识符关键字，2020-12 的 `$id`/`$anchor`/`$dynamicRef` 不参与解析，
与 2.x 时代 `RefResolver` 的认知一致。不会因为 jsonschema 4.x 默认支持
2020-12 而改变解析结果。

## 保持不变的行为（回归测试锁定）

以下行为经探针对比 2.6.0 基线确认一致，并有测试覆盖：

- 本地 `#/definitions/...`、store 内相对 ref（`references.json#/...`）、
  间接 ref（ref 的 ref）、`patternProperties`/`items`/`anyOf` 内 ref、
  `$ref` 与同级字段共存时字段保留（`tests/test_transform.py`
  `TestResolveSchemaReferences`）。
- `Decimal` → `singer.decimal` 字符串化（`test_decimal_transform_compat`、
  `test_decimal_types_transform`）。
- bool 与 integer 互转（`True`→`1`、`1`→`True`、`"false"`→`False`，
  `test_boolean_integer_transform_compat`）。
- anyOf 全部失败时的错误位置（属性路径 `["f"]` + 根路径 `[]`，
  `test_anyof_error_reports_outer_path`）。

## 验证方式

```bash
python -m pip install -r requirements-domain.lock
python -m pip install -e .
PYTHONUTF8=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -o addopts= \
  -o log_cli=false tests/test_catalog.py tests/test_state.py tests/test_schema.py \
  tests/test_singer.py tests/test_transform.py tests/test_metadata.py
```

测试输入全部为本地生成的内存/临时文件数据，不需要网络、账号或外部服务。
