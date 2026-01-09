FROM astrocrpublic.azurecr.io/runtime:3.0-9

# -------------------------------------------------------------------
# SYSTEM DEPENDENCIES (Root)
# -------------------------------------------------------------------
# Switch to root to install system packages
USER root

# 1. Add PostgreSQL apt repository
# 2. Import the signing key
# 3. Install postgresql-client-17
RUN apt-get update && apt-get install -y lsb-release wget gnupg2 && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && apt-get install -y postgresql-client-17

# Switch back to the 'astro' user for Python packages and runtime
USER astro

# -------------------------------------------------------------------
# PYTHON DEPENDENCIES (Astro User)
# -------------------------------------------------------------------
COPY requirements.txt .

# 3. Install the Python packages listed in the file
RUN pip install --no-cache-dir -r requirements.txt

# 4. Install dbt Core. This gives us the 'dbt' command inside our container.
RUN pip install --no-cache-dir dbt-core