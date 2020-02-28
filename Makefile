pull-and-run-image:
	docker pull docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.5
	docker tag docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.5 stripe-subscriptions-migrator
	docker-compose run stripe-subscriptions-migrator

build:
	docker-compose build stripe-subscriptions-migrator
	docker-compose run stripe-subscriptions-migrator

push:
	docker tag stripe-subscriptions-migrator docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.5
	docker push docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.5


build-with-cnb:
	pack build stripe-subscriptions-migrator --builder heroku/buildpacks
