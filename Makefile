VERSION=v0.9.12
pull-and-run-image:
	docker pull ghcr.io/newsrevenuehub/stripe-subscriptions-migrator:${VERSION}
	docker tag ghcr.io/newsrevenuehub/stripe-subscriptions-migrator:${VERSION} stripe-subscriptions-migrator
	docker-compose run stripe-subscriptions-migrator

build:
	docker-compose build stripe-subscriptions-migrator

run: build
	docker-compose run stripe-subscriptions-migrator

push: build
	docker tag stripe-subscriptions-migrator ghcr.io/newsrevenuehub/stripe-subscriptions-migrator:${VERSION}
	docker push ghcr.io/newsrevenuehub/stripe-subscriptions-migrator:${VERSION}


build-with-cnb:
	pack build stripe-subscriptions-migrator --builder heroku/buildpacks
