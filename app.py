import streamlit as st
import pandas as pd
import re
from collections import defaultdict
import io
import datetime
# 新增：文档解析库
from docx import Document
import PyPDF2

# ==========================================
# 核心架构层：Google Sheets 云端数据库引擎
# ==========================================
st.set_page_config(page_title="云端智能教研系统 V9", layout="wide")

# 【容灾机制】：尝试连接 Google Sheets，若无密钥则使用本地缓存
if 'db_papers' not in st.session_state:
    st.session_state.db_papers = pd.DataFrame(columns=["试卷名称", "学科", "上传时间", "试卷内容"])
if 'db_records' not in st.session_state:
    st.session_state.db_records = pd.DataFrame(columns=["标签", "题号条件", "学生名单", "总人数", "记录时间"])

USE_GSHEETS = False
try:
    from streamlit_gsheets import GSheetsConnection
    conn = st.connection("gsheets", type=GSheetsConnection)
    USE_GSHEETS = True
except Exception as e:
    st.sidebar.warning("⚠️ 未检测到 Google Sheets 密钥，系统已自动降级为【本地内存模式】。数据将在刷新后丢失。")

# --- 数据库操作封装 (CRUD) ---
def load_table(table_name):
    if USE_GSHEETS:
        try:
            return conn.read(worksheet=table_name, ttl=0).dropna(how="all")
        except:
            return pd.DataFrame() # 表格不存在或为空时的兜底
    else:
        return st.session_state[f'db_{table_name.lower()}']

def append_to_table(table_name, new_row_dict):
    if USE_GSHEETS:
        df_old = load_table(table_name)
        df_new = pd.concat([df_old, pd.DataFrame([new_row_dict])], ignore_index=True)
        conn.update(worksheet=table_name, data=df_new)
    else:
        state_key = f'db_{table_name.lower()}'
        st.session_state[state_key] = pd.concat([st.session_state[state_key], pd.DataFrame([new_row_dict])], ignore_index=True)

# ==========================================
# 辅助层：文件解析与数据清洗
# ==========================================
def extract_text_from_file(file):
    """滴水不漏的文档提词器：支持 Word 和 PDF"""
    text = ""
    try:
        if file.name.endswith('.docx'):
            doc = Document(file)
            text = "\n".join([p.text for p in doc.paragraphs if p.text.strip()])
        elif file.name.endswith('.pdf'):
            reader = PyPDF2.PdfReader(file)
            text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
        else:
            text = file.getvalue().decode('utf-8') # 兜底纯文本
    except Exception as e:
        return f"文件解析失败: {e}"
    return text

def parse_questions_to_set(q_str):
    if pd.isna(q_str): return set()
    return set(re.findall(r'\d+', str(q_str)))

def parse_names_to_set(name_str):
    if pd.isna(name_str) or str(name_str).strip() in ['无', '', 'nan']: return set()
    clean_str = re.sub(r'[、，,\s\x1a]+', ',', str(name_str))
    return set([n.strip() for n in clean_str.split(',') if n.strip()])

# ==========================================
# 交互界面：三大业务模块
# ==========================================
st.title("☁️ 试卷错题云端定位系统 (V9 SaaS版)")

tab1, tab2, tab3 = st.tabs(["📄 模块一：试卷原题入库", "🎯 模块二：错题精准分析", "🗄️ 模块三：云端数据库看板"])

# ------------------------------------------
# 模块一：试卷原稿上传与解析入库
# ------------------------------------------
with tab1:
    st.subheader("第一步：上传考试原卷 (支持 Word/PDF)")
    st.write("上传的试卷将被智能提取为纯文本，并永久归档至 Google Sheets，为后续接入 VIO 大模型做准备。")
    
    col_upload1, col_upload2 = st.columns(2)
    with col_upload1:
        paper_subject = st.selectbox("试卷所属学科：", ["高中化学", "高中物理", "高中生物", "其他"])
        paper_name = st.text_input("定义试卷名称：", placeholder="例如：2026届高三三月统考化学卷")
    with col_upload2:
        uploaded_paper = st.file_uploader("选择试卷文件", type=['docx', 'pdf', 'txt'])
        
    if st.button("📤 解析并存入云端题库", type="primary"):
        if not paper_name or not uploaded_paper:
            st.warning("请填写试卷名称并上传文件。")
        else:
            with st.spinner("正在启动文档解析引擎..."):
                extracted_content = extract_text_from_file(uploaded_paper)
                
                if "解析失败" in extracted_content:
                    st.error(extracted_content)
                elif len(extracted_content) < 10:
                    st.warning("提取到的文本过少，请检查文件是否为纯图片 PDF。")
                else:
                    # 组装数据并推送到云端
                    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    new_paper = {
                        "试卷名称": paper_name,
                        "学科": paper_subject,
                        "上传时间": now_str,
                        "试卷内容": extracted_content
                    }
                    append_to_table("Papers", new_paper)
                    st.success(f"✅ 试卷【{paper_name}】解析成功！共提取 {len(extracted_content)} 个字符，已同步至云端。")
                    
                    with st.expander("点击预览提取出的试卷文本"):
                        st.text(extracted_content[:1000] + "...\n\n(为节省空间，仅显示前1000字)")

# ------------------------------------------
# 模块二：错题精准分析与打标 (继承原核心逻辑)
# ------------------------------------------
with tab2:
    st.subheader("第二步：上传成绩单提取重点跟进名单")
    uploaded_excels = st.file_uploader("上传错题成绩表 (Excel)", type=['xlsx', 'xls'], accept_multiple_files=True)
    
    query_conditions = {}
    papers_data = {}

    if uploaded_excels:
        for i, file in enumerate(uploaded_excels):
            with st.expander(f"⚙️ 表格配置: {file.name}", expanded=False):
                try:
                    xls = pd.ExcelFile(file)
                    selected_sheet = st.selectbox("选择工作表", xls.sheet_names, key=f"s_{i}")
                    header_row = st.number_input("列名在第几行？", min_value=1, value=2, key=f"h_{i}") - 1
                    df_full = pd.read_excel(xls, sheet_name=selected_sheet, header=header_row)
                    
                    layout_type = st.radio("排版类型：", ["以学生为行", "以题号为行(包含合并单元格)"], key=f"l_{i}")
                    student_dict = defaultdict(set)
                    
                    if "学生" in layout_type:
                        name_col = st.selectbox("姓名列", df_full.columns, index=0, key=f"n_{i}")
                        err_col = st.selectbox("错题列", df_full.columns, index=1, key=f"e_{i}")
                        for _, row in df_full.iterrows():
                            if pd.notna(row[name_col]):
                                student_dict[str(row[name_col]).strip()].update(parse_questions_to_set(row[err_col]))
                    else:
                        q_col = st.selectbox("题号列", df_full.columns, index=0, key=f"q_{i}")
                        names_col = st.selectbox("答错名单列", df_full.columns, index=len(df_full.columns)-1, key=f"names_{i}")
                        df_full[q_col] = df_full[q_col].ffill()
                        for _, row in df_full.iterrows():
                            q_nums = re.findall(r'\d+', str(row[q_col]))
                            if q_nums:
                                for name in parse_names_to_set(row[names_col]):
                                    student_dict[name].add(q_nums[0])
                    papers_data[file.name] = dict(student_dict)
                except Exception as e:
                    st.error(f"解析 {file.name} 失败: {e}")
                    
            target_input = st.text_input("🎯 要求命中的错题号", placeholder="例: 2, 3", key=f"t_{i}")
            if target_input.strip():
                query_conditions[file.name] = parse_questions_to_set(target_input)

        if query_conditions:
            st.divider()
            threshold = st.number_input("满足几份试卷条件即输出？", min_value=1, max_value=len(query_conditions), value=len(query_conditions))
            
            if st.button("🔍 执行精准匹配"):
                all_students = set()
                for sd in papers_data.values(): all_students.update(sd.keys())
                hit_students = []
                for student in all_students:
                    match_count = sum(1 for p_name, t_qs in query_conditions.items() if t_qs.issubset(papers_data[p_name].get(student, set())))
                    if match_count >= threshold:
                        hit_students.append(student)

                if hit_students:
                    st.success(f"找到 {len(hit_students)} 位符合条件的学生。")
                    st.text_area("名单：", "、".join(hit_students), height=70)
                    
                    # 打标签并上传云端
                    tag = st.text_input("为此批名单打上知识点标签 (例: 阿伏伽德罗常数):")
                    if st.button("☁️ 永久保存至 Google Sheets", type="primary"):
                        if not tag:
                            st.warning("请先输入标签！")
                        else:
                            formatted_query = "；".join([f"[{p}]题号:{','.join(qs)}" for p, qs in query_conditions.items()])
                            new_record = {
                                "标签": tag,
                                "题号条件": formatted_query,
                                "学生名单": "、".join(hit_students),
                                "总人数": len(hit_students),
                                "记录时间": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            append_to_table("Records", new_record)
                            st.success("✅ 数据已安全写入云端数据库！")

# ------------------------------------------
# 模块三：云端数据库只读看板
# ------------------------------------------
with tab3:
    st.subheader("🗄️ 云端数据中心")
    if USE_GSHEETS:
        st.success("🟢 状态：已连接至 Google Sheets 云端数据库")
    else:
        st.warning("🟡 状态：本地内存模拟模式 (未连接 Google)")
        
    st.markdown("#### 📁 已归档试卷库 (Papers)")
    df_papers = load_table("Papers")
    if not df_papers.empty:
        # 隐藏长文本，只显示元数据
        st.dataframe(df_papers[["试卷名称", "学科", "上传时间"]], use_container_width=True)
    else:
        st.info("暂无试卷记录")

    st.markdown("#### 🏷️ 错题标签追踪库 (Records)")
    df_records = load_table("Records")
    if not df_records.empty:
        st.dataframe(df_records, use_container_width=True)
    else:
        st.info("暂无错题记录")
