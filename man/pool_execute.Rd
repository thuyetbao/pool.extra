% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pool_method.R
\name{pool_execute}
\alias{pool_execute}
\title{Pool Insert}
\usage{
pool_execute(pool, statement, ...)
}
\arguments{
\item{pool}{pool object, created by `pool::dbPool`}

\item{statement}{SQL statement send to database}

\item{...}{Other parameters passed on to method, align with `DBI` package}
}
\description{
method of excute statment to database managed by pool
}
\references{
\href{SQL Specifiation}{https://cran.r-project.org/web/packages/DBI/vignettes/spec.html}
\href{Issue 88 of Pool}{https://github.com/rstudio/pool/issues/88}
}
