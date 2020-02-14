build:
	docker-compose build stripe-subscriptions-migrator 
	docker-compose run stripe-subscriptions-migrator


build-with-cnb:
	pack build stripe-subscription-migrator --builder heroku/buildpacks