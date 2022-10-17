# Hello! This is Mungtage's pipeline

# Installation

```
conda env create -f environment.yml
```

# Installation (manual)

```
conda update -n base -c defaults conda
```

```
conda create -n mungtage_pipe python=3.8
```

```
conda activate mungtage_pipe
```

```
conda install -c conda-forge python-dotenv -y
```

```
conda install -c conda-forge pytz -y
```

```
conda install -c anaconda requests -y
```

```
conda install -c conda-forge tqdm -y
```

```
conda install -c anaconda pymysql -y
```

```
conda install -c conda-forge pprintpp -y
```

```
conda install -c anaconda pandas -y
```

# Usage

```
conda activate mungtage_pipe
```

```
python get_animals.py
```
