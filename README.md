YouTube Analytics Pipeline
Project Overview

This project aims to build a simple analytics pipeline to analyze the performance of YouTube videos using exported analytics data from YouTube Studio.

The goal is to transform raw analytics exports into structured datasets that allow analysis of video performance, engagement, and audience behavior.

The project simulates a typical analytics engineering workflow by ingesting raw data into a data warehouse and transforming it into analytical models.

Motivation

As a content creator, I wanted to better understand which videos perform best and what characteristics drive engagement and subscriber growth.

Some of the questions this project aims to answer include:

Which videos generate the highest engagement?

Which videos convert viewers into subscribers?

Does video duration affect watch percentage?

What factors correlate with higher video performance?

Data Source

The data comes from YouTube Studio analytics exports, which provide video-level performance metrics such as:

Views

Watch time

Average view duration

Likes

Comments

Subscribers gained

Impressions

Click-through rate (CTR)

The data is exported as CSV files from YouTube Studio.

Project Architecture

The pipeline follows a simple analytics workflow:

YouTube Analytics Export (CSV)
            ↓
Python Ingestion Script
            ↓
BigQuery Data Warehouse (raw layer)
            ↓
dbt Transformations
            ↓
Analytics Models
            ↓
Video Performance Analysis
