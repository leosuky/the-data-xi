FROM astrocrpublic.azurecr.io/runtime:3.0-9

COPY requirements.txt .

# 3. Install the Python packages listed in the file
RUN pip install --no-cache-dir -r requirements.txt

# 4. Install dbt Core. This gives us the 'dbt' command inside our container.
RUN pip install --no-cache-dir dbt-core==1.9.1