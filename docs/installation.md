# Installation

sdag is not available on pypi yet (it's coming soon!). If you manage dependencies with [uv](https://docs.astral.sh/uv/), you can directly install sdag from [GitHub](https://github.com/igeniusai/sdag):

```sh
uv add "sdag @ git+https://github.com/igeniusai/sdag"
```

You can also install sdag as a tool to gain global access to it:

```sh
uv tool install "sdag @ git+https://github.com/igeniusai/sdag"
```

To install sdag with pip, first clone the GitHub repository:

```sh
git clone https://github.com/igeniusai/sdag.git
```

and then install sdag:

```sh
pip install path/to/sdag
```

If you face issues in the process, you likely have to install [Rust](https://rust-lang.org/tools/install/) or update it to a more recent version via:

```sh
rustup update
```
