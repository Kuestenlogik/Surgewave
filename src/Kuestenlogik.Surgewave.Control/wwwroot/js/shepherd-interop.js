window.surgewaveShepherd = {
    startTour: function (tourDef) {
        var tour = new Shepherd.Tour({
            useModalOverlay: true,
            defaultStepOptions: {
                classes: 'surgewave-shepherd-step',
                scrollTo: { behavior: 'smooth', block: 'center' },
                cancelIcon: { enabled: true }
            }
        });

        tourDef.steps.forEach(function (step, i) {
            var buttons = [];
            if (i > 0) {
                buttons.push({ text: 'Back', action: tour.back, secondary: true });
            }
            if (i < tourDef.steps.length - 1) {
                buttons.push({ text: 'Next', action: tour.next });
            } else {
                buttons.push({ text: 'Done', action: tour.complete });
            }

            var config = {
                id: step.id,
                title: step.title,
                text: step.text,
                buttons: buttons
            };

            if (step.element) {
                config.attachTo = { element: step.element, on: step.position || 'bottom' };
            }

            tour.addStep(config);
        });

        tour.on('complete', function () {
            localStorage.setItem('surgewave_shepherd_' + tourDef.id, 'true');
        });

        tour.start();
    },

    isTourCompleted: function (tourId) {
        return localStorage.getItem('surgewave_shepherd_' + tourId) === 'true';
    },

    resetTourProgress: function () {
        Object.keys(localStorage)
            .filter(function (k) { return k.startsWith('surgewave_shepherd_'); })
            .forEach(function (k) { localStorage.removeItem(k); });
    }
};
