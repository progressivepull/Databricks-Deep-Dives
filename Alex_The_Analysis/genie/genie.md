# **Genie**

**Databricks Genie (Natural Language AI)**

<img src="./media/image1.png" style="width:6.5in;height:2.51389in" />

- Genie allows users to **query data using plain English**.

- Designed mainly for **non-technical/business users**.

- Can:

  - Explain datasets

  - Show sample data

  - Answer analytical questions (e.g., most common pickup zip code)

- Generates **SQL queries automatically** and shows the code.

- Can create **basic visualizations**, but results depend heavily on how
  you phrase prompts.

------------------------------------------------------------------------

**Working with Data (Taxi Dataset Example)**

**Click New button.**

<img src="./media/image2.png" style="width:6.5in;height:3.24653in" />

<img src="./media/image3.png" style="width:6.5in;height:6.60764in" />

**You can Ask questions about the data set.**

<img src="./media/image4.png" style="width:6.5in;height:3.65486in" />

Dataset includes:

<img src="./media/image5.png" style="width:6.5in;height:5.77569in" />

- Pickup/drop-off times

- Trip distance

- Fare amount

- Zip codes

<!-- -->

- Example insights:

  - Most common pickup zip code identified via grouping/count.

  - Peak ride time identified (e.g., 6 PM).

# Custom Instructions in Databricks Genie

<img src="./media/image6.png" style="width:6.5in;height:7.84444in" />

**Custom Instructions in Databricks Genie let you *control how Genie
behaves*, including tone, coding style, preferred libraries, and
workspace‑wide rules. They work through special Markdown files that
Genie reads automatically and applies to all your interactions.**

**✨ What You Can Put in User Instructions**

- Your preferred libraries

  - *“Use PySpark instead of pandas unless I say otherwise.”*

- Your coding style

  - *“Always include comments explaining each step.”*

- Your role or context

  - *“I am a data engineer working on Delta Live Tables.”*

- Tone

  - *“Use a concise, professional tone.”*

- Guidelines

  - *“Always return SQL and Python versions of queries.”*

# Settings in Databricks Genie

<img src="./media/image7.png" style="width:6.21319in;height:9in" />

In **Databricks Genie Settings**, **Sample Questions** are *example
natural‑language questions* that you provide to Genie so it can learn
how users typically ask about your data. Genie uses these examples to
improve accuracy, reduce ambiguity, and generate better SQL for your
domain.

<img src="./media/image8.png" style="width:6.5in;height:6.03056in" />

------------------------------------------------------------------------

**Prompting Matters**

<img src="./media/image9.png" style="width:6.5in;height:3.84653in" />

<img src="./media/image10.png" style="width:6.5in;height:2.47083in" />

- Vague prompts → less useful outputs.

> <img src="./media/image11.png" style="width:6.5in;height:2.77847in" />

- More specific prompts → better visualizations and results.

- View the SQL that Genie generated from your prompt. You can edit the
  SQL and rerun the query.

> <img src="./media/image12.png" style="width:6.5in;height:3.82014in" />

- Example:

  - Picked time for Zip Code

<img src="./media/image13.png" style="width:6.5in;height:3.48264in" />

<img src="./media/image14.png" style="width:6.5in;height:2.65694in" />  
<img src="./media/image15.png" style="width:6.5in;height:3.87569in" />

- “Show all pickup hours and counts” → better chart

<img src="./media/image16.png" style="width:6.5in;height:0.62361in" />

<img src="./media/image17.png" style="width:6.5in;height:2.27361in" />

<img src="./media/image18.png" style="width:6.5in;height:3.55556in" />

Genie will also visualize data.

- Example: If you write prompt “Can you write a SQL query to show me all
  the pickup times and counts and then visualize it?”

<img src="./media/image19.png" style="width:6.5in;height:2.81528in" />

<img src="./media/image20.png" style="width:6.5in;height:2.06944in" />

<img src="./media/image21.png" style="width:6.5in;height:2.51667in" />

------------------------------------------------------------------------

**SQL Editor AI Assistant**

<img src="./media/image22.png" style="width:6.5in;height:2.34792in" />

- Two assistant types:

  - **Code generator** (bottom)

  - **Context-aware assistant** (top right)

- Capabilities:

  - Generate SQL queries

  - Fix errors automatically

  - Explain and document code

  - Modify queries (e.g., calculate averages, convert units)

- Example:

  - Calculated **average trip duration** using pickup/drop-off
    timestamps.

------------------------------------------------------------------------

**AI-Assisted Coding Features**

- Slash commands (/) enable quick actions:

  - Explain code

  - Optimize

  - Fix errors

  - Add comments

- Can iteratively refine queries (e.g., seconds → minutes).

## Summary

**🧩 1. Code generator (Genie Code)**

This is the **developer‑focused** Genie.

You’ll see it as the floating assistant on the right side of the SQL
Editor.

**What it does**

- Helps you **write or edit SQL**

- Explains queries

- Suggests fixes

- Generates code from natural language

- Works with your **Custom Instructions** (your
  .assistant_instructions.md)

**🧩 2. Context-aware assistant** (**Genie BI / Q&A)**

This is the **business‑intelligence Genie** used for:

- Natural‑language questions over datasets

- Auto‑generated dashboards

- Semantic understanding of tables

- Sample Questions (in Genie Spaces)

**🧠 The Simple Mental Model**

Think of it like this:

- **Code generator (Genie Code) = your coding assistant**

- **Context-aware assistant** (**Genie BI/Q&A) = your data‑question
  assistant**

------------------------------------------------------------------------

**Notebook AI Assistant**

<img src="./media/image23.png"
style="width:2.66975in;height:2.1532in" />

<img src="./media/image24.png" style="width:6.5in;height:1.48819in" />

<img src="./media/image25.png" style="width:6.5in;height:2.7875in" />

- Supports multiple languages:

  - Python, SQL, R, Markdown

- Can:

  - Generate code

  - Convert between languages (e.g., Python → SQL)

<img src="./media/image26.png" style="width:6.5in;height:2.9in" />

- Useful for **data exploration and transformation**.

------------------------------------------------------------------------

**AI in Dashboards**

- AI can create visualizations via prompts:

  - Example: “Create a card with average fare”

<img src="./media/image27.png" style="width:6.5in;height:4.15556in"
alt="Graphical user interface, application AI-generated content may be incorrect." />

<img src="./media/image28.png" style="width:2.84737in;height:3.43768in"
alt="Graphical user interface, application AI-generated content may be incorrect." />

- Example: “Create a line chart of pickup counts”

> <img src="./media/image29.png" style="width:6.5in;height:3.37569in"
> alt="A screenshot of a computer AI-generated content may be incorrect." />
>
> <img src="./media/image30.png" style="width:2.92376in;height:3.48629in"
> alt="Graphical user interface, chart AI-generated content may be incorrect." />

- Can modify charts with natural language:

  - Change granularity (daily → hourly)

<img src="./media/image31.png" style="width:6.5in;height:3.32708in" />

<img src="./media/image32.png" style="width:6.5in;height:3.14514in"
alt="Graphical user interface, chart AI-generated content may be incorrect." />

- Add labels or adjust axes

<img src="./media/image33.png" style="width:6.5in;height:2.40417in"
alt="Chart, line chart AI-generated content may be incorrect." />

<img src="./media/image34.png" style="width:6.5in;height:3.07986in"
alt="A screenshot of a computer AI-generated content may be incorrect." />

- Still allows **manual customization after generation**.

------------------------------------------------------------------------

**Key Takeaways**

- Databricks integrates AI across:

  - Genie (natural language queries)

  - SQL Editor (coding + debugging)

  - Notebooks (multi-language workflows)

  - Dashboards (visualization creation)

- AI is most useful for:

  - Speeding up workflows

  - Assisting non-technical users

  - Debugging and learning

- However, **clear prompts and user understanding** still matter for
  best results.

# [Content](./../content.md)