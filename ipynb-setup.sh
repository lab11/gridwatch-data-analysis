echo 'export PATH="$PATH:'$(pwd)/ipython-cleaner'"' >> ~/.bashrc
source ~/.bashrc
mkdir -p ~/.config
mkdir -p ~/.config/git
touch ~/.config/git/attributes
echo '*.ipynb  filter=clean_ipynb' >> ~/.config/git/attributes
git config --global filter.clean_ipynb.clean ipynb_drop_output.py
git config --global filter.clean_ipynb.smudge cat
