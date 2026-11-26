import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
import streamlit as st
from dotenv import load_dotenv
from typing import List, Callable
import pandas as pd
import os
import re
import time
from custom_logging import logger

# Papers are analyzed in batches of this size per LLM call (token-limit
# workaround), sleeping between calls to respect API rate limits.
CHUNK_SIZE = 500
SLEEP_SECONDS = 30

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

# Retry policy for Gemini 429s (ResourceExhausted). Per-minute limits clear
# within a minute, so a few waits usually get through; anything longer is
# surfaced to the UI as QuotaExceededError rather than a raw traceback.
MAX_QUOTA_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 20
MAX_RETRY_DELAY_SECONDS = 90


class QuotaExceededError(Exception):
    """Gemini kept returning ResourceExhausted after retrying (or the quota
    is one that retrying can't fix, like a daily or zero limit)."""


def _retry_delay_seconds(exc: Exception, attempt: int) -> float:
    """Prefer the delay Gemini asks for: RetryInfo in the gRPC error
    details, else "retry in 23.5s" in the message; fall back to doubling
    a default. Capped so a Streamlit run never hangs for minutes."""
    delay = None
    for detail in getattr(exc, "details", None) or []:
        retry_delay = getattr(detail, "retry_delay", None)
        if retry_delay is not None:
            delay = retry_delay.seconds + retry_delay.nanos / 1e9
            break
    if delay is None:
        match = re.search(r"retry in ([\d.]+)\s*s", str(exc), re.IGNORECASE)
        if match:
            delay = float(match.group(1))
    if delay is None:
        delay = DEFAULT_RETRY_DELAY_SECONDS * (2 ** attempt)
    return min(max(delay, 1.0), MAX_RETRY_DELAY_SECONDS)


def _is_unretryable_quota(exc: Exception) -> bool:
    """Daily quotas and zero limits (model not available on the key's tier)
    won't clear in the next minute, so waiting on them only stalls the UI."""
    text = str(exc).lower()
    return "perday" in text or "per_day" in text or "limit: 0" in text


def _generate(prompt: str):
    """model.generate_content with retry on quota (429) errors."""
    for attempt in range(MAX_QUOTA_RETRIES + 1):
        try:
            return model.generate_content(prompt)
        except google_exceptions.ResourceExhausted as exc:
            if _is_unretryable_quota(exc) or attempt == MAX_QUOTA_RETRIES:
                logger.error(f"Gemini quota exhausted, giving up after {attempt + 1} attempt(s): {exc}")
                raise QuotaExceededError(str(exc)) from exc
            delay = _retry_delay_seconds(exc, attempt)
            logger.warning(
                f"Gemini quota hit (attempt {attempt + 1}/{MAX_QUOTA_RETRIES + 1}); retrying in {delay:.0f}s."
            )
            time.sleep(delay)


def run_chunked_extraction(
    df: pd.DataFrame,
    extract_fn: Callable[[pd.DataFrame], str],
    chunk_size: int = CHUNK_SIZE,
    sleep_seconds: int = SLEEP_SECONDS,
) -> list[str]:
    """Splits df into row-chunks and calls extract_fn(chunk) once per chunk,
    sleeping between calls (never after the last one) to respect API rate
    limits. Shared by the State of the Art and Custom Question flows, which
    only differ in which extract_fn they pass in."""
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    chunks = [c for c in chunks if not c.empty]
    results = []
    for i, chunk in enumerate(chunks):
        results.append(extract_fn(chunk))
        if i < len(chunks) - 1:
            time.sleep(sleep_seconds)
    return results


def summarize_topic_evolution(df: pd.DataFrame, topic_name) -> str:
    """
    Summarize how the topic evolved over time using top 3 papers per year.
    Assumes df contains: title, abstract, year

    Intentionally a single LLM call, not chunked like the State of the
    Art / Custom Question flows: the UI caps this at ~50 papers/year over
    ~8 years (~400 rows max), well within a Flash-class model's context
    window at typical abstract lengths — and chunking would break the
    by-year narrative that's the point of this summary (a flat row-chunker
    would split individual years across calls).
    """
    if len(df) > 200:
        logger.warning(
            f"summarize_topic_evolution got {len(df)} rows for topic '{topic_name}' — "
            "well above the ~150-200 row range this was designed for; the summary "
            "may be less detailed than usual."
        )

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

    response = _generate(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while generating topic evaluation summary for {topic_name}.")

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

    response = _generate(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Extracted key points using {tokens_in} input and {tokens_out} output tokens for {topic_name} after {cutoff_year}.")

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

    response = _generate(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while synthesizing state of the art summary for {topic_name} after {cutoff_year}.")

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

    response = _generate(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Extracted relevant info using {tokens_in} input and {tokens_out} output tokens for question '{question}' in {topic_name} after {cutoff_year}.")

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

    response = _generate(prompt)
    tokens_in = response.usage_metadata.prompt_token_count
    tokens_out = response.usage_metadata.candidates_token_count

    logger.info(f"Used {tokens_in} input and {tokens_out} output tokens while synthesizing answer for question '{question}' in {topic_name} after {cutoff_year}.")

    return response.text

