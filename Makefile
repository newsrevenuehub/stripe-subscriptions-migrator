pull-and-run-image:
	docker pull docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.1
	docker tag docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.1 stripe-subscriptions-migrator
	docker-compose run stripe-subscriptions-migrator

build:
	docker-compose build stripe-subscriptions-migrator
	docker-compose run stripe-subscriptions-migrator


build-with-cnb:
	pack build stripe-subscriptions-migrator --builder heroku/buildpacks
