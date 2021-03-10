pip-tools:
	pip install -U pip
	pip install pip-tools

requirements: pip-tools
	pip-compile requirements.in > requirements.txt
	pip-sync

dev-requirements: pip-tools
	pip-compile dev-requirements.in > dev-requirements.txt
	pip-sync

prepare:
	black .
	isort .