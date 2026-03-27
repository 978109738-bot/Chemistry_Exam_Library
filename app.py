import streamlit as st
import pandas as pd
import re

# 【严密逻辑1：数据清洗函数】提取字符串中的所有数字，并转换为集合
def parse_questions_to_set(q_str):
    if pd.isna(q_str):
        return set()
    # 将输入的内容强制转换为字符串，并使用正则表达式提取所有数字
    # 这样可以完全无视用户输入的是顿号、逗号还是空格
    numbers = re.findall(r'\d+', str(q_str))
    return set(numbers)

st.title("多试卷错题精准定位系统")
st.write("请上传各版试卷的Excel表格，并设定需要检索的错题号。")

# 1. 动态文件上传区
uploaded_files = st.file_uploader("上传Excel文件（可多选）", type=['xlsx', 'xls'], accept_multiple_files=True)

if uploaded_files:
    st.subheader("设置错题检索条件")
    
    # 用于存储用户对每张试卷的检索要求
    query_conditions = {}
    # 用于存储清洗后的试卷数据
    papers_data = {}
    
    # 2. 动态生成输入交互界面，并预处理数据
    for file in uploaded_files:
        filename = file.name
        # 假设A列是姓名(索引为0)，B列是错题号(索引为1)
        try:
            df = pd.read_excel(file, usecols=[0, 1], names=['姓名', '错题号'])
            df.dropna(subset=['姓名'], inplace=True) # 剔除空行
            
            # 将该试卷的数据转换为字典: {姓名: {错题号集合}}
            student_dict = {}
            for _, row in df.iterrows():
                name = str(row['姓名']).strip()
                student_dict[name] = parse_questions_to_set(row['错题号'])
            
            papers_data[filename] = student_dict
            
        except Exception as e:
            st.error(f"读取 {filename} 失败，请检查格式是否为A列姓名、B列错题。报错信息: {e}")
            st.stop()
            
        # 生成前端输入框
        target_input = st.text_input(f"请输入【{filename}】要求同时命中的错题号（如没有要求请留空）", 
                                     placeholder="例如: 2, 3, 6", key=filename)
        if target_input.strip():
            # 将用户的输入也统一转化为数字集合
            query_conditions[filename] = parse_questions_to_set(target_input)

    # 3. 执行核心交集运算逻辑
    if st.button("开始精准匹配", type="primary"):
        if not query_conditions:
            st.warning("您还没有输入任何错题条件！")
        else:
            # 收集所有出现在任何一张试卷中的学生姓名
            all_students = set()
            for student_dict in papers_data.values():
                all_students.update(student_dict.keys())
            
            hit_students = []
            
            # 【严密逻辑2：多条件交集判定】
            for student in all_students:
                is_match = True # 假设该学生符合条件
                
                for paper_name, target_qs in query_conditions.items():
                    # 如果该学生没考这张试卷，或者错题集不包含目标错题集
                    student_wrong_qs = papers_data[paper_name].get(student, set())
                    
                    # 判断 target_qs 是否是 student_wrong_qs 的子集
                    if not target_qs.issubset(student_wrong_qs):
                        is_match = False
                        break # 只要有一张试卷不满足，直接淘汰，检查下一个学生
                
                if is_match:
                    hit_students.append(student)
            
            # 4. 结果输出
            st.divider()
            if hit_students:
                st.success(f"匹配成功！共找到 {len(hit_students)} 位符合所有条件的学生：")
                # 以美观的标签形式展示
                st.write("、".join(hit_students))
            else:
                st.info("没有找到符合上述所有条件的学生。")