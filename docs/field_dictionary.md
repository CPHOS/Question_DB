# 字段字典

## papers
- `paper_id`: 试卷唯一 ID。
- `edition`: 届数，不再默认使用自然年份。
- `paper_type`: 试卷类型，推荐使用 `regular`、`semifinal`、`final`、`other`，分别对应常规、复赛、决赛和其他。
- `title`: 试卷标题。
- `paper_latex_path`: 试卷主 LaTeX 文件来源路径，用于追溯原始工程文件。
- `paper_latex_source`: 试卷主 LaTeX 文本快照，导入时直接写入数据库。
- `source_pdf_path`: 可选的编译后 PDF 或历史 PDF 路径。
- `question_index_json`: 试卷内题目索引，按 `paper_index` 保存题目顺序和 LaTeX 引用。
- `notes`: 内部备注。

## questions
- `question_id`: 题目唯一 ID。
- `paper_id`: 所属试卷。
- `paper_index`: 题目在卷内的顺序索引，用它而不是页码定位。
- `question_no`: 展示用题号。
- `category`: `theory` 或 `experiment`。
- `latex_path`: 题目主体 LaTeX 文件来源路径。
- `latex_source`: 题目主体 LaTeX 文本快照。
- `answer_latex_path`: 解答或答案 LaTeX 文件来源路径。
- `answer_latex_source`: 解答或答案 LaTeX 文本快照。
- `latex_anchor`: 题目在 LaTeX 中的锚点或逻辑索引。
- `search_text`: 用于检索和去重的文本索引，可人工指定，也可由 LaTeX 内容派生。
- `status`: `raw`、`reviewed`、`published`。
- `tags_json`: 标签数组。
- `notes`: 题目备注。

## question_assets
- `asset_id`: 资产唯一 ID。
- `kind`: `statement_image`、`answer_image`、`figure`。
- `file_path`: 资产在服务器文件系统中的来源路径或相对索引路径。
- `sha256`: 文件哈希。
- `caption`: 图注。
- `sort_order`: 同一道题多个图片时的顺序。

## score_workbooks
- `workbook_id`: xlsx 工作簿唯一 ID。
- `paper_id`: 所属试卷。
- `exam_session`: 场次标签。
- `workbook_kind`: 工作簿类型，例如 `paper_registry`、`score_table`。
- `source_filename`: 原始文件名。
- `file_path`: 工作簿在服务器文件系统中的来源路径或相对索引路径。
- `sheet_names_json`: 工作表名称列表。
- `file_size`: 文件字节数。
- `sha256`: 工作簿哈希。
- `workbook_blob`: xlsx 二进制内容，直接写入服务器数据库。
- `notes`: 备注。

## question_stats
- `exam_session`: 统计所属场次。
- `source_workbook_id`: 统计来源的 xlsx 工作簿 ID。
- `participant_count`: 参与人数。
- `avg_score`: 平均分。
- `score_std`: 标准差。
- `full_mark_rate`: 满分率。
- `zero_score_rate`: 零分率。
- `max_score`: 满分。
- `min_score`: 最低分。
- `stats_source`: 统计来源标签。
- `stats_version`: 统计版本号。

## difficulty_scores
- `manual_level`: 人工难度标签。
- `derived_score`: 规则算法得分，范围 0 到 1。
- `method`: 算法名称。
- `method_version`: 算法版本。
- `confidence`: 置信度。
- `feature_json`: 用于生成难度的统计摘要。
