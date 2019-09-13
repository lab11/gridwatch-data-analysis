mkdir -p ~/.config
mkdir -p ~/.config/git
touch ~/.config/git/attributes
echo '*.ipynb  filter=clean_ipynb' >> ~/.config/git/attributes
git config filter.clean_ipynb.clean ./ipython-cleaner/ipynb_drop_output.py
git config filter.clean_ipynb.smudge cat
