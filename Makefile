pull-and-run-image:
	docker login docker.pkg.github.com
	docker pull docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.7
	docker tag docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.7 stripe-subscriptions-migrator
	docker-compose run stripe-subscriptions-migrator

build:
	docker-compose build stripe-subscriptions-migrator

run: build
	docker-compose run stripe-subscriptions-migrator

push: build
	docker tag stripe-subscriptions-migrator docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.7
	docker push docker.pkg.github.com/newsrevenuehub/stripe-subscriptions-migrator/stripe-subscriptions-migrator:v0.9.7


build-with-cnb:
	pack build stripe-subscriptions-migrator --builder heroku/buildpacks
