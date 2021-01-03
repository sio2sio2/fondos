module = fondos
compress = xz
docdir = doc  # Directorio de documetación

current_dir = $(notdir $(shell pwd))
target = ${module}.tar.${compress}

clean:
	@find . -type d \( -name "__pycache__" -o -name ".mypy_cache" \) -print0 | xargs -0 rm -rf

pkg: ${target}

%.tar.${compress}: clean
	tar -acvf ../$@ ../${current_dir}
