
df2 <- arrow::open_dataset(file.path(lake_directory,"forecasts/parquet/site_id=fcre")) |>
  filter(model_id == "ltreb",
         depth == 1.5) |>
  collect()

df3 <- arrow::open_dataset(file.path(lake_directory,"scores/parquet/site_id=fcre")) |>
  filter(model_id == "ltreb",
         depth == 1.5) |>
  collect() #|>
         #datetime < "2023-03-26")


df_forecast <- df2 |>
  filter(reference_datetime > "2023-01-01",
         parameter < 8,
         variable %in% c("temperature","chla",'oxygen','fdom'))
         #variable %in% c("temperature","chla"), depth == 1.5)


p_forecast <- df_forecast |>
  ggplot(aes(x=datetime, y=prediction, group= parameter)) +
  geom_line(aes(color = variable)) +
  geom_vline(xintercept = as_datetime("2023-03-26")) +
  facet_wrap(~variable, scales = 'free')

p_forecast


##scores
df_scores <- df3 |>
  filter(reference_datetime > "2023-01-01",
         variable %in% c("temperature","chla",'oxygen','fdom'))

p_scores <- df_scores |>
  ggplot(aes(x=datetime, y=median)) +
  geom_ribbon(aes(ymin=quantile02.5, ymax = quantile97.5), fill = 'grey70') +
  geom_line(aes(x=datetime,y=median)) +
  geom_point(aes(x = datetime,  y = observation), color = "black", size = 0.8, shape = 3) +
  geom_vline(xintercept = as_datetime("2023-03-26")) +
    facet_wrap(~variable, scales = 'free')

p_scores

#geom_ribbon 97.5 aned 2.5
#put on median of geom)line ontop of that
#geom_point for observations
