import google.generativeai as genai
import streamlit as st
from dotenv import load_dotenv
from typing import List
import pandas as pd
import os
from custom_logging import logger, log_llm_usage
import sqlite3
from datetime import datetime

# Load environment variables from .env file
load_dotenv()


def _get_config(key: str) -> str:
    """Local dev reads from .env via os.environ; Streamlit Cloud has no .env
    file and injects secrets via st.secrets instead — fall back to that."""
    if key in os.environ:
        return os.environ[key]
    if key in st.secrets:
        return st.secrets[key]
    raise KeyError(f"'{key}' not found in environment variables or Streamlit secrets")


genai.configure(api_key=_get_config("GOOGLE_API_KEY"))
model = genai.GenerativeModel(_get_config("GOOGLE_API_MODEL"))

def summarize_topic_evolution(df: pd.DataFrame, topic_name) -> str:
    """
    Summarize how the topic evolved over time using top 3 papers per year.
    Assumes df contains: title, abstract, year
    """
    yearly_chunks = []
    for year in sorted(df["year"].unique()):
        papers = df[df["year"] == year]
        abstracts = "\n\n".join(
            f"Title: {row['title']}\nAbstract: {row['Abstract']}"
            for _, row in papers.iterrows()
        )
        yearly_chunks.append(f"--- Year: {year} ---\n{abstracts}")

    prompt = f"""
    You are a machine learning expert. Analyze the following abstracts of research papers organized by year.

    Summarize the major developments and evolution in the topics across the years in the papers. 

    Focus on shifts in research direction, recurring themes, notable milestones, or any pattern in the hypotheses or techniques.

    Don't create a detailed summary but rather a high-level overview of how the topic has evolved over time.

    {chr(10).join(yearly_chunks)}
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while generating topic evaluation summary for {topic_name}.")
    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text


def extract_key_points_state_of_art(df: pd.DataFrame, cutoff_year: int, topic_name: str) -> str:
    """
    Extract key points from papers for state of the art analysis.
    Lightweight extraction to stay under token limits.
    """
    papers_text = "\n\n".join(
        f"Title: {row['title']}\nAbstract: {row['Abstract']}"
        for _, row in df.iterrows()
    )

    prompt = f"""
    Extract key research points from these papers (after year {cutoff_year}) in concise bullet format:

    - Core hypotheses and ideas
    - Novel techniques or discoveries
    - Limitations and open questions
    - Trade-offs in approaches
    - Shared assumptions or constraints
    - Convergence, redundancy, or saturation signs
    - Competing directions or disagreements
    - Benchmarks used
    - Under-explored angles

    Be concise. Output only organized bullet points.

    {papers_text}
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Extracted key points using {tokens_in} input and {tokens_out} output tokens for {topic_name} after {cutoff_year}.")
    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text


def synthesize_state_of_art(extracted_points: list, topic_name: str, cutoff_year: int) -> str:
    """
    Synthesize extracted key points into a single, comprehensive state-of-the-art summary.
    """
    joined = "\n\n---\n\n".join(extracted_points)
    prompt = f"""
        You are an expert research analyst. Below are extracted key points from research papers released after the year {cutoff_year}.

        Your task is to synthesize these into a single, comprehensive summary for a researcher new to the field.
        Remove redundancy, integrate evidence and insights, and ensure your summary is well-structured and critical.

        **Your summary must explicitly address the following points:**
        - The core hypotheses and ideas being explored
        - Novel techniques or discoveries introduced
        - Common limitations, failure cases, or open questions
        - Trade-offs involved in current approaches
        - Any shared assumptions or constraints
        - Signs of convergence, redundancy, or saturation
        - Disagreements or competing directions in the field
        - Benchmarks used to support claims and their realism
        - Under-explored or neglected angles that deserve attention

        Based on this, provide a critical synthesis: where is the field at right now? How mature is it? Is there evidence of overhype or real transformation and future directions?

        Here are the extracted key points:
        {joined}
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while synthesizing state of the art summary for {topic_name} after {cutoff_year}.")
    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text


def extract_relevant_info_for_question(question: str, df: pd.DataFrame, cutoff_year: int, topic_name: str) -> str:
    """
    Extract information relevant to answering a custom question.
    Lightweight extraction to stay under token limits.
    """
    papers_text = "\n\n".join(
        f"Title: {row['title']}\nAbstract: {row['Abstract']}"
        for _, row in df.iterrows()
    )

    prompt = f"""
    Extract information from these papers (after year {cutoff_year}) that is relevant to answering this question: "{question}"

    Focus on:
    - Direct evidence or findings related to the question
    - Relevant techniques, hypotheses, or ideas
    - Important context or background
    - Limitations or caveats
    - Competing perspectives if any

    Be concise. Output only relevant bullet points.

    {papers_text}
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Extracted relevant info using {tokens_in} input and {tokens_out} output tokens for question '{question}' in {topic_name} after {cutoff_year}.")
    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text


def synthesize_answer_from_extracts(extracted_info: list, question: str, topic_name: str, cutoff_year: int) -> str:
    """
    Synthesize extracted information into a single, comprehensive answer to the custom question.
    """
    joined = "\n\n---\n\n".join(extracted_info)
    prompt = f"""
    You are an expert research analyst. Below is extracted information from research papers released after the year {cutoff_year}.

    Your task is to synthesize this information into a single, comprehensive answer to the question: "{question}"

    Remove redundancy, integrate evidence and insights, and ensure your answer is clear, well-structured, and insightful.

    **While answering, make sure to cover these aspects as relevant to the question:**
    - The core hypotheses and ideas being explored
    - Novel techniques or discoveries introduced
    - Common limitations, failure cases, or open questions
    - Trade-offs involved in current approaches
    - Any shared assumptions or constraints
    - Signs of convergence, redundancy, or saturation
    - Disagreements or competing directions in the field
    - Benchmarks used to support claims and their realism
    - Under-explored or neglected angles that deserve attention

    Here is the extracted information:
    {joined}
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while synthesizing answer for question '{question}' in {topic_name} after {cutoff_year}.")
    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text

