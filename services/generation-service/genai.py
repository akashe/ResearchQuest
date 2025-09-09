import google.generativeai as genai
from dotenv import load_dotenv
from typing import List
import pandas as pd
import os
from custom_logging import logger, log_llm_usage
import sqlite3
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
model = genai.GenerativeModel(os.environ["GOOGLE_API_MODEL"])

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


def summarize_state_of_art(df: pd.DataFrame, cutoff_year: int, topic_name) -> str:
    """
    Summarize the state of the art after a particular year.
    Assumes df contains: title, abstract, year
    """
    papers_text = "\n\n".join(
        f"Title: {row['title']}\nAbstract: {row['Abstract']}"
        for _, row in df.iterrows()
    )

    prompt = f"""
    You are a researcher analyzing the state of the art in a machine learning topic after the year {cutoff_year}.

    Using the research abstracts below, summarize the following:

    - The core hypotheses and ideas being explored
    - Novel techniques or discoveries introduced
    - Common limitations, failure cases, or open questions
    - Trade-offs involved in current approaches
    - Any shared assumptions or constraints
    - Signs of convergence, redundancy, or saturation
    - Disagreements or competing directions in the field
    - Benchmarks used to support claims and their realism
    - Under-explored or neglected angles that deserve attention

    Based on this, provide a critical synthesis: where is the field at right now? How mature is it? Is there evidence of overhype or real transformation and future directions?”

    {papers_text}
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while generating state of the art summary for {topic_name} after {cutoff_year}.")

    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text


def combine_summaries(results: list, topic_name: str, cutoff_year: int) -> str:
    """
    Combine multiple state-of-the-art summaries into a single, coherent synthesis.
    """
    joined = "\n\n---\n\n".join(results)
    prompt = f"""
        You are an expert research analyst. Below are several partial summaries of the state of the art from research papers released after the year {cutoff_year} using their titles and abstract.

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

        Here are the partial summaries:
        {joined}
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while combining state of the art summaries for {topic_name} after {cutoff_year}.")
    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text


def ask_custom_question(question, df: pd.DataFrame, cutoff_year: int, topic_name) -> str:
    """
    Summarize the state of the art after a particular year.
    Assumes df contains: title, abstract, year
    """
    papers_text = "\n\n".join(
        f"Title: {row['title']}\nAbstract: {row['Abstract']}"
        for _, row in df.iterrows()
    )

    prompt = f"""
    You are a researcher analyzing the state of the art in AI and machine learning. You will be given abstracts and titles of most important papers after the year {cutoff_year}.

    Using the research abstracts below, understand the following for yourself:

    - The core hypotheses and ideas being explored
    - Novel techniques or discoveries introduced
    - Common limitations, failure cases, or open questions
    - Trade-offs involved in current approaches
    - Any shared assumptions or constraints
    - Signs of convergence, redundancy, or saturation
    - Disagreements or competing directions in the field
    - Benchmarks used to support claims and their realism
    - Under-explored or neglected angles that deserve attention

    Here is the information about paper titles and their abstracts: {papers_text}

    Based on this, answer a specific question: "{question}"
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while generating state of the art summary for {topic_name} after {cutoff_year}.")

    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text


def combine_answers(results: list, question: str, topic_name: str, cutoff_year: int) -> str:
    """
    Combine multiple answers to a custom question into a single, coherent answer.
    """
    joined = "\n\n---\n\n".join(results)
    prompt = f"""
    You are an expert research analyst. Below are several partial answers to the question "{question}" generated from paper released after the year {cutoff_year} using their titles and abstract.

    Your task is to synthesize these into a single, comprehensive answer. 
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

    Here are the partial answers:
    {joined}
    """

    response = model.generate_content(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while combining answers for question '{question}' in {topic_name} after {cutoff_year}.")
    log_llm_usage(topic_name, prompt, response.text, tokens_in, tokens_out)

    return response.text

