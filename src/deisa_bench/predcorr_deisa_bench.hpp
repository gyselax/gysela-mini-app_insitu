// SPDX-License-Identifier: MIT

#pragma once

#include "geometry_xyvxvy.hpp"
#include "itimesolver.hpp"

class IQNSolver;
class IVlasovSolver;

/**
 * @brief Predictor-corrector time solver for the in-situ benchmark app.
 *
 * At every iteration, fdistribu is copied to the host in the (species, x, y, vx, vy)
 * layout and exposed to PDI with the "iteration" event, and once more after the last
 * iteration with the "last_iteration" event, for the in-situ analytics wired up in
 * pdi_out_diags.yaml. The host copy is read-only for the PDI handlers.
 */
class PredCorrDeisaBench : public ITimeSolver
{
private:
    IVlasovSolver const& m_vlasov_solver;

    IQNSolver const& m_poisson_solver;

public:
    /**
     * @brief Creates an instance of the predictor-corrector class.
     * @param[in] vlasov_solver A solver for a Boltzmann equation.
     * @param[in] poisson_solver A solver for a Poisson equation.
     */
    PredCorrDeisaBench(IVlasovSolver const& vlasov_solver, IQNSolver const& poisson_solver);

    ~PredCorrDeisaBench() override = default;

    /**
     * @brief Solves the Vlasov-Poisson system.
     * @param[in, out] allfdistribu On input : the initial value of the distribution function.
     *                              On output : the value of the distribution function after solving
     *                              the Vlasov-Poisson system a given number of iterations.
     * @param[in] dt The timestep.
     * @param[in] steps The number of iterations to be performed by the predictor-corrector.
     * @return The distribution function after solving the system.
     */
    DFieldSpVxVyXY operator()(DFieldSpVxVyXY allfdistribu, Real dt, int steps = 1) const override;
};
