# 初始 schema

`src.models.Base.metadata.create_all()` 按外键顺序建表，由 `src.main` 在 API 首次启动时执行。已有开发卷不执行自动字段迁移。项目 README 记录了模型与首次建库入口。

## 撤销案件字段

`delay_incidents` 新增 `withdraw_reason`、`withdraw_requested_by`、`withdraw_requested_at`、`withdrawn_by`、`withdrawn_at`，状态枚举新增 `withdraw_review` 与 `withdrawn`。已有开发卷需按上述列手动 `ALTER TABLE`，或重建 `database` 卷后重新 seed。
